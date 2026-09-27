const GEMINI_API_KEY = "";
const TELEGRAM_TOKEN = "";

/** Script cache max TTL (6 hours). */
const CACHE_TTL_SEC = 21600;
const DEDUP_CACHE_PREFIX = "upd_";
const JOB_RESULT_PREFIX = "job_result_";
const QUEUE_PROPERTY_KEY = "pending_update_ids";
const JOB_PROPERTY_PREFIX = "job_";
const PROCESSOR_HANDLER = "processJobQueue";

/**
 * Telegram webhook: dedupe, enqueue, return OK immediately.
 */
function doPost(e) {
  const okOutput = ContentService.createTextOutput("OK").setMimeType(
    ContentService.MimeType.TEXT
  );

  try {
    if (!e || !e.postData || !e.postData.contents) {
      return okOutput;
    }

    const update = JSON.parse(e.postData.contents);
    if (!update.message || !update.message.text || update.message.from.is_bot) {
      return okOutput;
    }

    const updateId = update.update_id;
    const chatId = update.message.chat.id;
    const userText = update.message.text;

    if (!claimUpdateForProcessing(updateId)) {
      return okOutput;
    }

    enqueueJob({
      updateId: updateId,
      chatId: chatId,
      userText: userText,
      isStart: userText === "/start",
    });
    scheduleJobProcessor();
  } catch (err) {
    Logger.log("Ошибка в doPost: " + err.toString());
  }

  return okOutput;
}

/**
 * Atomic dedup: lock + cache so parallel webhook retries see the same update_id once.
 */
function claimUpdateForProcessing(updateId) {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(10000)) {
    return false;
  }

  try {
    const cache = CacheService.getScriptCache();
    const key = DEDUP_CACHE_PREFIX + updateId;
    if (cache.get(key)) {
      return false;
    }
    cache.put(key, "1", CACHE_TTL_SEC);
    return true;
  } finally {
    lock.releaseLock();
  }
}

function enqueueJob(job) {
  const props = PropertiesService.getScriptProperties();
  props.setProperty(JOB_PROPERTY_PREFIX + job.updateId, JSON.stringify(job));

  const queue = JSON.parse(props.getProperty(QUEUE_PROPERTY_KEY) || "[]");
  if (queue.indexOf(job.updateId) === -1) {
    queue.push(job.updateId);
    props.setProperty(QUEUE_PROPERTY_KEY, JSON.stringify(queue));
  }
}

function scheduleJobProcessor() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    return;
  }

  try {
    const existing = ScriptApp.getProjectTriggers().filter(function (t) {
      return t.getHandlerFunction() === PROCESSOR_HANDLER;
    });
    if (existing.length === 0) {
      ScriptApp.newTrigger(PROCESSOR_HANDLER).timeBased().after(1000).create();
    }
  } finally {
    lock.releaseLock();
  }
}

/**
 * Runs outside the webhook (installable time trigger). Heavy work happens here.
 */
function processJobQueue() {
  deleteTriggersForHandler(PROCESSOR_HANDLER);

  const props = PropertiesService.getScriptProperties();
  let queue = JSON.parse(props.getProperty(QUEUE_PROPERTY_KEY) || "[]");

  while (queue.length > 0) {
    const updateId = queue.shift();
    props.setProperty(QUEUE_PROPERTY_KEY, JSON.stringify(queue));

    const raw = props.getProperty(JOB_PROPERTY_PREFIX + updateId);
    props.deleteProperty(JOB_PROPERTY_PREFIX + updateId);
    if (!raw) {
      continue;
    }

    const job = JSON.parse(raw);
    try {
      handleQueuedJob(job);
    } catch (err) {
      Logger.log("processJobQueue job " + updateId + ": " + err.toString());
      sendTelegramMessage(job.chatId, "❌ Ошибка обработки: " + err.toString());
    }
  }
}

function handleQueuedJob(job) {
  if (job.isStart) {
    sendTelegramMessage(
      job.chatId,
      "👋 Привет! Я твой AI-ассистент. Напиши, какой проект или задачу нужно создать."
    );
    return;
  }

  const cachedResult = CacheService.getScriptCache().get(
    JOB_RESULT_PREFIX + job.updateId
  );
  if (cachedResult) {
    sendTelegramMessage(job.chatId, formatAgentReply(JSON.parse(cachedResult)));
    return;
  }

  sendTelegramMessage(job.chatId, "⏳ Запрос принят, обрабатываю...");

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
  return "🤖 *Ответ:* " + agentResult;
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
      return "Gemini не вернула вариантов ответа.";
    }

    const part = json.candidates[0].content.parts[0];

    if (part.functionCall && part.functionCall.name === "createFolderAndTask") {
      const projectName = part.functionCall.args.projectName;
      return executeCreateFolderAndTask(projectName, updateId);
    }

    if (part.text) {
      return part.text;
    }

    return "Запрос обработан.";
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
  const url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage";
  UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify({
      chat_id: chatId,
      text: text,
      parse_mode: "Markdown",
      disable_web_page_preview: false,
    }),
  });
}

function deleteTriggersForHandler(handlerName) {
  ScriptApp.getProjectTriggers().forEach(function (trigger) {
    if (trigger.getHandlerFunction() === handlerName) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

/**
 * СБРОС СТАРОЙ ОЧЕРЕДИ И ТРИГГЕРОВ (Запустить 1 раз вручную!)
 */
function hardReset() {
  const triggers = ScriptApp.getProjectTriggers();
  for (let i = 0; i < triggers.length; i++) {
    ScriptApp.deleteTrigger(triggers[i]);
  }

  PropertiesService.getScriptProperties().deleteProperty(QUEUE_PROPERTY_KEY);

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
