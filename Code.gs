const GEMINI_API_KEY = "";
const TELEGRAM_TOKEN = "";

/** Script cache max TTL (6 hours). */
const CACHE_TTL_SEC = 21600;
const DEDUP_CACHE_PREFIX = "upd_";
const JOB_RESULT_PREFIX = "job_result_";
const QUEUE_PROPERTY_KEY = "pending_update_ids";
const JOB_PROPERTY_PREFIX = "job_";
const PROCESSOR_HANDLER = "processJobQueue";
const POLL_HANDLER = "pollJobQueue";
const POLL_WORKER_FLAG = "poll_worker_installed";
/** Fast-path jobs only in webhook; Gemini runs in time-based triggers. */
const MAX_QUICK_JOBS_PER_WEBHOOK = 20;

const SMALL_TALK_REPLY =
  "👋 Привет! Я помогаю создавать проекты: папка на Google Drive и задача в Google Tasks.\n\n" +
  "Напишите, например: «Создай проект Название»";

const AGENT_SYSTEM_INSTRUCTION =
  "Ты бот Telegram. Твоя основная функция — по запросу пользователя создать папку на Google Drive и задачу в Google Tasks (инструмент createFolderAndTask). " +
  "Вызывай createFolderAndTask только когда пользователь явно просит создать проект, задачу или папку. " +
  "Если сообщение — приветствие, болтовня, вопрос не про создание проекта или команда непонятна — ответь коротким текстом (без вызова инструмента): " +
  "объясни, что ты умеешь создавать проекты (папка + задача), и попроси написать название проекта.";

/**
 * Telegram webhook: dedupe, enqueue, return OK immediately.
 */
function doPost(e) {
  const okOutput = ContentService.createTextOutput("OK").setMimeType(
    ContentService.MimeType.TEXT
  );

  try {
    ensurePollWorkerTrigger();

    if (!e || !e.postData || !e.postData.contents) {
      return okOutput;
    }

    const update = JSON.parse(e.postData.contents);
    if (!update.message || !update.message.text || update.message.from.is_bot) {
      return okOutput;
    }

    const updateId = update.update_id;
    const chatId = update.message.chat.id;
    const userText = update.message.text.trim();

    let claim = claimUpdateForProcessing(updateId);
    if (claim === "busy") {
      Utilities.sleep(2000);
      claim = claimUpdateForProcessing(updateId);
    }
    if (claim === "duplicate") {
      return okOutput;
    }
    if (claim === "busy") {
      Logger.log("claim busy for update " + updateId);
      return okOutput;
    }

    const isStart = userText === "/start";
    const isSmallTalk = !isStart && isSmallTalkMessage(userText);
    enqueueJob({
      updateId: updateId,
      chatId: chatId,
      userText: userText,
      isStart: isStart,
      isSmallTalk: isSmallTalk,
    });

    if (!isStart && !isSmallTalk) {
      sendTelegramMessage(chatId, "⏳ Запрос принят, обрабатываю...");
    }

    drainJobQueue(MAX_QUICK_JOBS_PER_WEBHOOK, true);
    if (getQueueLength() > 0) {
      scheduleJobProcessor();
    }
  } catch (err) {
    Logger.log("Ошибка в doPost: " + err.toString());
  }

  return okOutput;
}

/**
 * @returns {"new"|"duplicate"|"busy"}
 */
function claimUpdateForProcessing(updateId) {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) {
    return "busy";
  }

  try {
    const cache = CacheService.getScriptCache();
    const key = DEDUP_CACHE_PREFIX + updateId;
    if (cache.get(key)) {
      return "duplicate";
    }
    cache.put(key, "1", CACHE_TTL_SEC);
    return "new";
  } finally {
    lock.releaseLock();
  }
}

function releaseUpdateDedup(updateId) {
  CacheService.getScriptCache().remove(DEDUP_CACHE_PREFIX + updateId);
}

function enqueueJob(job) {
  withScriptLock(15000, function () {
    const props = PropertiesService.getScriptProperties();
    props.setProperty(JOB_PROPERTY_PREFIX + job.updateId, JSON.stringify(job));

    const queue = JSON.parse(props.getProperty(QUEUE_PROPERTY_KEY) || "[]");
    if (queue.indexOf(job.updateId) === -1) {
      queue.push(job.updateId);
      props.setProperty(QUEUE_PROPERTY_KEY, JSON.stringify(queue));
    }
  });
}

function getQueueLength() {
  const queue = JSON.parse(
    PropertiesService.getScriptProperties().getProperty(QUEUE_PROPERTY_KEY) || "[]"
  );
  return queue.length;
}

function scheduleJobProcessor() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    return;
  }

  try {
    if (getQueueLength() === 0) {
      return;
    }
    const existing = ScriptApp.getProjectTriggers().filter(function (t) {
      return t.getHandlerFunction() === PROCESSOR_HANDLER;
    });
    if (existing.length < 3) {
      ScriptApp.newTrigger(PROCESSOR_HANDLER).timeBased().after(1000).create();
    }
  } catch (err) {
    Logger.log("scheduleJobProcessor: " + err.toString());
  } finally {
    lock.releaseLock();
  }
}

function withScriptLock(maxWaitMs, fn) {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(maxWaitMs)) {
    return null;
  }
  try {
    return fn();
  } finally {
    lock.releaseLock();
  }
}

function isQuickJob(job) {
  return job.isStart || job.isSmallTalk;
}

function dequeueNextJob(quickOnly) {
  return withScriptLock(15000, function () {
    const props = PropertiesService.getScriptProperties();

    while (true) {
      const queue = JSON.parse(props.getProperty(QUEUE_PROPERTY_KEY) || "[]");
      if (queue.length === 0) {
        return null;
      }

      const updateId = queue[0];
      const raw = props.getProperty(JOB_PROPERTY_PREFIX + updateId);
      if (!raw) {
        queue.shift();
        props.setProperty(QUEUE_PROPERTY_KEY, JSON.stringify(queue));
        continue;
      }

      const job = JSON.parse(raw);
      if (quickOnly && !isQuickJob(job)) {
        return null;
      }

      queue.shift();
      props.setProperty(QUEUE_PROPERTY_KEY, JSON.stringify(queue));
      props.deleteProperty(JOB_PROPERTY_PREFIX + updateId);
      return job;
    }
  });
}

/**
 * Lock is held only while dequeuing — Gemini/Drive run without blocking other webhooks.
 */
function drainJobQueue(maxJobs, quickOnly) {
  const limit = maxJobs === undefined ? 50 : maxJobs;
  const onlyQuick = quickOnly === true;
  let processed = 0;

  while (processed < limit) {
    const job = dequeueNextJob(onlyQuick);
    if (!job) {
      break;
    }
    processed++;

    try {
      handleQueuedJob(job);
    } catch (err) {
      Logger.log("drainJobQueue job " + job.updateId + ": " + err.toString());
      releaseUpdateDedup(job.updateId);
      sendTelegramMessage(
        job.chatId,
        "❌ Ошибка обработки. Попробуйте отправить сообщение ещё раз.\n\n" +
          err.toString()
      );
    }
  }
}

function processJobQueue() {
  drainJobQueue();
  deleteTriggersForHandler(PROCESSOR_HANDLER);
  if (getQueueLength() > 0) {
    scheduleJobProcessor();
  }
}

function isSmallTalkMessage(text) {
  const normalized = text.toLowerCase().trim();
  if (normalized.length > 80) {
    return false;
  }
  if (/^(привет|здравствуй|здарова|хай|hello|hi|hey|ку)[\s,!?.—-]*/i.test(normalized)) {
    return true;
  }
  return /^(как дела|как ты|что делаешь|как поживаешь|как сам)[\s,!?.—-]*/i.test(
    normalized
  );
}

function handleQueuedJob(job) {
  if (job.isStart) {
    sendTelegramMessage(
      job.chatId,
      "👋 Привет! Я твой AI-ассистент. Напиши, какой проект или задачу нужно создать."
    );
    return;
  }

  if (job.isSmallTalk) {
    sendTelegramMessage(job.chatId, SMALL_TALK_REPLY);
    return;
  }

  const cachedResult = CacheService.getScriptCache().get(
    JOB_RESULT_PREFIX + job.updateId
  );
  if (cachedResult) {
    sendTelegramMessage(job.chatId, formatAgentReply(JSON.parse(cachedResult)));
    return;
  }

  const agentResult = runAgent(job.userText, job.updateId);
  CacheService.getScriptCache().put(
    JOB_RESULT_PREFIX + job.updateId,
    JSON.stringify(agentResult),
    CACHE_TTL_SEC
  );

  sendTelegramMessage(job.chatId, formatAgentReply(agentResult));
}

function formatAgentReply(agentResult) {
  if (typeof agentResult === "object" && agentResult.status === "success") {
    return (
      "✅ *Проект успешно создан!*\n\n" +
      "📁 *Папка на Диске:* [Открыть](" +
      agentResult.folderUrl +
      ")\n" +
      "📌 *Задача в Google Tasks:* " +
      agentResult.folderName
    );
  }

  if (typeof agentResult === "object" && agentResult.status === "unrecognized") {
    return (
      "🤷 *Не удалось распознать команду.*\n\n" +
      "Я умею создавать *проект*: папку на Google Drive и задачу в Google Tasks.\n\n" +
      "Напишите, например: _Создай проект Ремонт кухни_"
    );
  }

  if (typeof agentResult === "string" && agentResult.length > 0) {
    return "🤖 " + agentResult;
  }

  return (
    "🤷 *Не понял запрос.*\n\n" +
    "Опишите проект, который нужно создать (папка на Drive + задача в Google Tasks)."
  );
}

/**
 * Gemini API + function calling.
 */
function runAgent(userPrompt, updateId) {
  const url =
    "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.8-flash:generateContent?key=" +
    GEMINI_API_KEY;

  const tools = [
    {
      functionDeclarations: [
        {
          name: "createFolderAndTask",
          description:
            "Создает папку на Google Диске и задачу в Google Tasks со ссылкой на эту папку.",
          parameters: {
            type: "OBJECT",
            properties: {
              projectName: {
                type: "STRING",
                description: "Название проекта или задачи",
              },
            },
            required: ["projectName"],
          },
        },
      ],
    },
  ];

  const payload = {
    systemInstruction: {
      parts: [{ text: AGENT_SYSTEM_INSTRUCTION }],
    },
    contents: [
      {
        role: "user",
        parts: [{ text: userPrompt }],
      },
    ],
    tools: tools,
  };

  const options = {
    method: "post",
    contentType: "application/json",
    headers: {
      Accept: "application/json",
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(url, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();

    if (responseCode === 429) {
      Logger.log("⚠️ Превышен лимит запросов Gemini API (429).");
      return "⚠️ Достигнут лимит бесплатных запросов Gemini (20 запр/день). Попробуйте позже или подлючите Billing в Google AI Studio.";
    }

    if (responseCode !== 200) {
      Logger.log("❌ Ошибка API (" + responseCode + "): " + responseText);
      return "Ошибка Gemini API (" + responseCode + "): " + responseText;
    }

    const json = JSON.parse(responseText);

    if (!json.candidates || json.candidates.length === 0) {
      return { status: "unrecognized" };
    }

    const parts = json.candidates[0].content && json.candidates[0].content.parts;
    if (!parts || parts.length === 0) {
      return { status: "unrecognized" };
    }

    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];

      if (part.functionCall) {
        if (part.functionCall.name === "createFolderAndTask") {
          const args = part.functionCall.args || {};
          const projectName = (args.projectName || "").trim();
          if (!projectName) {
            return {
              status: "unrecognized",
              reason: "empty_project_name",
            };
          }
          return executeCreateFolderAndTask(projectName, updateId);
        }
        return { status: "unrecognized", tool: part.functionCall.name };
      }

      if (part.text && part.text.trim()) {
        return part.text.trim();
      }
    }

    return { status: "unrecognized" };
  } catch (e) {
    Logger.log("❌ Исключение: " + e.toString());
    return "Ошибка выполнения: " + e.toString();
  }
}

/**
 * Idempotent folder + task creation per Telegram update_id.
 */
function executeCreateFolderAndTask(projectName, updateId) {
  const cache = CacheService.getScriptCache();
  const resultKey = "folder_task_" + updateId;

  const cached = cache.get(resultKey);
  if (cached) {
    return JSON.parse(cached);
  }

  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const cachedAgain = cache.get(resultKey);
    if (cachedAgain) {
      return JSON.parse(cachedAgain);
    }

    const folder = DriveApp.createFolder(projectName);
    const folderUrl = folder.getUrl();

    const task = Tasks.newTask();
    task.title = projectName;
    task.notes = "Папка проекта на Google Drive:\n" + folderUrl;

    const createdTask = Tasks.Tasks.insert(task, "@default");

    const result = {
      status: "success",
      folderName: projectName,
      folderUrl: folderUrl,
      taskId: createdTask.id,
    };

    cache.put(resultKey, JSON.stringify(result), CACHE_TTL_SEC);
    return result;
  } finally {
    lock.releaseLock();
  }
}

function sendTelegramMessage(chatId, text) {
  if (trySendTelegram(chatId, text, "Markdown")) {
    return;
  }
  if (trySendTelegram(chatId, text, null)) {
    return;
  }
  Logger.log("sendTelegramMessage failed for chat " + chatId);
}

function trySendTelegram(chatId, text, parseMode) {
  const url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage";
  const body = {
    chat_id: chatId,
    text: text,
    disable_web_page_preview: false,
  };
  if (parseMode) {
    body.parse_mode = parseMode;
  }

  const response = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(body),
    muteHttpExceptions: true,
  });
  const code = response.getResponseCode();
  if (code !== 200) {
    Logger.log(
      "trySendTelegram (" + (parseMode || "plain") + ") " + code + ": " + response.getContentText()
    );
    return false;
  }
  return true;
}

function deleteTriggersForHandler(handlerName) {
  ScriptApp.getProjectTriggers().forEach(function (trigger) {
    if (trigger.getHandlerFunction() === handlerName) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

function ensurePollWorkerTrigger() {
  const props = PropertiesService.getScriptProperties();
  if (props.getProperty(POLL_WORKER_FLAG) === "1") {
    return;
  }

  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    return;
  }

  try {
    if (props.getProperty(POLL_WORKER_FLAG) === "1") {
      return;
    }

    const existing = ScriptApp.getProjectTriggers().filter(function (t) {
      return t.getHandlerFunction() === POLL_HANDLER;
    });
    if (existing.length === 0) {
      ScriptApp.newTrigger(POLL_HANDLER).timeBased().everyMinutes(1).create();
    }
    props.setProperty(POLL_WORKER_FLAG, "1");
  } catch (err) {
    Logger.log("ensurePollWorkerTrigger: " + err.toString());
  } finally {
    lock.releaseLock();
  }
}

/**
 * СБРОС СТАРОЙ ОЧЕРЕДИ И ТРИГГЕРОВ (Запустить 1 раз вручную!)
 */
function hardReset() {
  const triggers = ScriptApp.getProjectTriggers();
  for (let i = 0; i < triggers.length; i++) {
    ScriptApp.deleteTrigger(triggers[i]);
  }

  const props = PropertiesService.getScriptProperties();
  props.deleteProperty(QUEUE_PROPERTY_KEY);
  props.deleteProperty(POLL_WORKER_FLAG);
  const keys = props.getKeys();
  for (let i = 0; i < keys.length; i++) {
    if (keys[i].indexOf(JOB_PROPERTY_PREFIX) === 0) {
      props.deleteProperty(keys[i]);
    }
  }

  const WEB_APP_URL =
    "https://script.google.com/macros/s/AKfycbzv1T_buK84F5IQOGW5gLWVsqR0cpo69RCOttPs7J__-lbe1udT-jQOeISsWoxbazeo/exec";
  const url =
    "https://api.telegram.org/bot" +
    TELEGRAM_TOKEN +
    "/setWebhook?url=" +
    WEB_APP_URL +
    "&drop_pending_updates=true";
  const res = UrlFetchApp.fetch(url);
  Logger.log("Полная очистка завершена: " + res.getContentText());
}

function clearTelegramQueue() {
  const dropUrl =
    "https://api.telegram.org/bot" +
    TELEGRAM_TOKEN +
    "/deleteWebhook?drop_pending_updates=true";
  const res1 = UrlFetchApp.fetch(dropUrl);
  Logger.log("Очистка очереди Telegram: " + res1.getContentText());

  Utilities.sleep(2000);

  const WEB_APP_URL =
    "https://script.google.com/macros/s/AKfycbzv1T_buK84F5IQOGW5gLWVsqR0cpo69RCOttPs7J__-lbe1udT-jQOeISsWoxbazeo/exec";
  const setUrl =
    "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/setWebhook?url=" + WEB_APP_URL;
  const res2 = UrlFetchApp.fetch(setUrl);
  Logger.log("Повторная установка Webhook: " + res2.getContentText());
}

function testRunner() {
  const result = runAgent("Создай проект Проверка Таймаута", 999999001);
  Logger.log("Результат: " + JSON.stringify(result, null, 2));
}

/** Manual fallback if auto-install of poll worker failed (run once from editor). */
function setupAsyncWorkers() {
  PropertiesService.getScriptProperties().deleteProperty(POLL_WORKER_FLAG);
  ensurePollWorkerTrigger();
  Logger.log("pollJobQueue trigger ensured (every 1 minute).");
}

function pollJobQueue() {
  if (getQueueLength() === 0) {
    return;
  }
  drainJobQueue(10);
  if (getQueueLength() > 0) {
    scheduleJobProcessor();
  }
}

function reloadWebhook() {
  const WEB_APP_URL =
    "https://script.google.com/macros/s/AKfycbzv1T_buK84F5IQOGW5gLWVsqR0cpo69RCOttPs7J__-lbe1udT-jQOeISsWoxbazeo/exec";

  const deleteUrl =
    "https://api.telegram.org/bot" +
    TELEGRAM_TOKEN +
    "/deleteWebhook?drop_pending_updates=true";
  UrlFetchApp.fetch(deleteUrl);

  Utilities.sleep(1000);

  const setUrl =
    "https://api.telegram.org/bot" +
    TELEGRAM_TOKEN +
    "/setWebhook?url=" +
    WEB_APP_URL +
    "&drop_pending_updates=true";
  const response = UrlFetchApp.fetch(setUrl);

  Logger.log("Результат обновления Webhook: " + response.getContentText());
}
