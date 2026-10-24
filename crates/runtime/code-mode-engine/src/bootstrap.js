((createCodeTools, valueHelpers) => {
  const { stringify, storeSnapshot, normalizeImage, normalizeAudio, generatedImageItems } = valueHelpers;
  const nativeTool = __nanocodeTool;
  const nativeContent = __nanocodeContent;
  const nativeLoad = __nanocodeLoad;
  const nativeNotify = __nanocodeNotify;
  const nativeYield = __nanocodeYield;
  const nativeSetTimeout = __nanocodeSetTimeout;
  const nativeClearTimeout = __nanocodeClearTimeout;
  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
  const jsonParse = JSON.parse;
  const jsonStringify = JSON.stringify;
  const cloneValue = typeof globalThis.structuredClone === "function"
    ? globalThis.structuredClone
    : (value) => value === undefined
      ? undefined
      : jsonParse(jsonStringify(value));

  function errorText(error) {
    if (!error) return String(error);
    const message = error.message;
    const label = message && error.name ? `${error.name}: ${message}` : message;
    if (error.stack && label && !error.stack.startsWith(label)) {
      return `${label}\n${error.stack}`;
    }
    return error.stack || label || String(error);
  }

  function failureText(payload) {
    if (typeof payload === "string") return payload;
    if (payload === null || typeof payload !== "object") return payload === undefined ? "" : jsonStringify(payload);
    if (Array.isArray(payload.content)) {
      const texts = payload.content
        .map((item) => typeof item?.text === "string" ? item.text : "")
        .filter(Boolean);
      if (texts.length) return texts.join("\n");
    }
    for (const key of ["message", "error", "detail"]) {
      if (typeof payload[key] === "string" && payload[key]) return payload[key];
    }
    return jsonStringify(payload) ?? "";
  }

  function toolFailure(name, payload) {
    if (payload instanceof Error) return payload;
    const message = failureText(payload) || `tool ${name} failed`;
    const error = new Error(message);
    Object.defineProperty(error, "message", { value: message, enumerable: true, configurable: true, writable: true });
    error.stack = `Error: ${message}`;
    error.tool = name;
    if (payload !== null && typeof payload === "object") {
      for (const key of Object.keys(payload)) {
        if (key !== "message" && key !== "name" && key !== "stack") error[key] = payload[key];
      }
      Object.defineProperty(error, "detail", { value: payload, configurable: true, writable: true });
    }
    return error;
  }

  function storageKey(value, helper) {
    try {
      return `${value}`;
    } catch {
      throw `${helper} key must be a string`;
    }
  }

  return async function runCell(source, definitionsJson, schemasJson, preludeSource) {
    const definitions = jsonParse(definitionsJson);
    let schemas;
    const stored = new Map();
    const storedWrites = new Map();
    const declaredTools = Object.create(null);
    const invokeTool = (name, input, host = false) => {
      const encodedInput = input === undefined ? "null" : jsonStringify(input);
      return nativeTool(name, encodedInput, host)
        .then(jsonParse, (payload) => Promise.reject(toolFailure(name, jsonParse(payload))));
    };
    for (const definition of definitions) {
      declaredTools[definition.name] = (input) => invokeTool(
        definition.tool_name,
        input === undefined && definition.kind === "function" ? {} : input,
      );
    }
    const tools = createCodeTools(
      Object.keys(declaredTools),
      (name, input) => declaredTools[name](input),
    );

    function text(value) {
      nativeContent(jsonStringify({ type: "input_text", text: stringify(value) }));
    }

    function notify(value) {
      const notification = stringify(value);
      if (!notification.trim()) throw "notify expects non-empty text";
      nativeNotify(notification);
    }

    function image(value, detail) {
      nativeContent(jsonStringify(normalizeImage(value, detail)));
    }

    function audio(value) {
      nativeContent(jsonStringify(normalizeAudio(value)));
    }

    function generatedImage(value) {
      for (const item of generatedImageItems(value)) {
        nativeContent(jsonStringify(item));
      }
    }

    function store(key, value) {
      const normalizedKey = storageKey(key, "store");
      const [, normalizedValue] = storeSnapshot(normalizedKey, value);
      stored.set(normalizedKey, normalizedValue);
      storedWrites.set(normalizedKey, normalizedValue);
    }

    function load(key) {
      const normalizedKey = storageKey(key, "load");
      if (stored.has(normalizedKey)) return cloneValue(stored.get(normalizedKey));
      const encoded = nativeLoad(normalizedKey);
      return encoded === undefined ? undefined : jsonParse(encoded);
    }

    function yield_control() {
      nativeYield();
    }

    const EXIT = Symbol("exit");
    function exit() { throw EXIT; }

    const allTools = Object.freeze(definitions.map((tool) => {
      return Object.freeze({
        name: tool.name,
        description: tool.description,
      });
    }));
    function toolSchema(name) {
      if (typeof name !== "string" || !name) {
        throw "toolSchema expects a non-empty tool name";
      }
      const index = definitions.findIndex((tool) =>
        tool.name === name || tool.tool_name === name
      );
      if (index < 0) return undefined;
      schemas ??= jsonParse(schemasJson);
      const [inputSchema, outputSchema] = schemas[index];
      return cloneValue({
        inputSchema: inputSchema ?? null,
        outputSchema: outputSchema ?? null,
      });
    }
    try {
      const prelude = preludeSource
        ? new Function("tools", "call", preludeSource)(
          tools,
          (name, input) => invokeTool(name, input, true),
        ) ?? {}
        : {};
      const preludeNames = Object.keys(prelude);
      const script = new AsyncFunction(
        "tools",
        "ALL_TOOLS",
        "toolSchema",
        "text",
        "image",
        "audio",
        "generatedImage",
        "notify",
        "store",
        "load",
        "yield_control",
        "exit",
        "setTimeout",
        "clearTimeout",
        ...preludeNames,
        "return (async () => {" + source + "\n})();",
      );
      try {
        await script(
          tools,
          allTools,
          toolSchema,
          text,
          image,
          audio,
          generatedImage,
          notify,
          store,
          load,
          yield_control,
          exit,
          nativeSetTimeout,
          nativeClearTimeout,
          ...preludeNames.map((name) => prelude[name]),
        );
      } catch (error) {
        if (error !== EXIT) throw error;
      }
      return jsonStringify({
        type: "done",
        stored: Object.fromEntries(storedWrites),
      });
    } catch (error) {
      return jsonStringify({
        type: "error",
        message: errorText(error),
        stored: Object.fromEntries(storedWrites),
      });
    }
  };
})
