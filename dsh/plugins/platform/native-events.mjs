/** The native renderer receives only product-visible events, never request envelopes. */
export function nativeEvent(event) {
  const { type, seq, time, data } = event;
  const base = { type, seq, time };
  if (type === "user/message" && data?.source?.kind === "user")
    return { ...base, surfaceOp: "append", data: { id: data.id, role: "user", source: { kind: "user" }, content: text(data.content) } };
  if (type === "assistant/message")
    return { ...base, surfaceOp: "append", data: { turn: data.turn, step: data.step,
      // `stream` is the raw compact records of the settlement. The product never
      // forwards them (request envelopes and raw payloads stay on the host), but the
      // 0.1.5 token meter reads `event.data.usage ?? streamUsage(event.data.stream)`
      // and throws on `undefined.length`, which kills the whole event feed. An empty
      // array is the honest value: no records forwarded, so no usage is displayed —
      // we never invent token counts.
      stream: [],
      message: { id: data.message?.id, role: "assistant", source: { kind: data.message?.source?.kind ?? "model", provider: data.message?.source?.provider, model: data.message?.source?.model }, content: (data.message?.content ?? []).flatMap((part) => part.type === "tool-call" ? [{ type: "tool-call", id: part.id, name: part.name, arguments: "{}" }] : text([part])) } } };
  if (type === "assistant/chunk" && data?.chunk?.type === "text-delta")
    return { ...base, data: { turn: data.turn, step: data.step, chunk: { type: "text-delta", text: data.chunk.text } } };
  if (["turn/start", "turn/end", "step/start", "step/end"].includes(type))
    return { ...base, data: { turn: data?.turn, step: data?.step, reason: data?.reason } };
  if (type === "tool/call") return { ...base, data: { turn: data.turn, step: data.step, name: data.name, callId: data.callId, arguments: "{}" } };
  if (type === "tool/result") {
    const callId = data.message?.source?.callId ?? data.message?.toolCallId;
    const isError = Boolean(data.error || data.message?.content?.some((part) => part.type === "tool-result" && part.isError));
    return { ...base, surfaceOp: "append", data: { turn: data.turn, step: data.step,
      message: { role: "tool", source: { kind: "tool", callId }, content: [{ type: "tool-result", toolCallId: callId,
        isError, content: [{ type: "text", text: isError ? "工具执行失败，请查看任务状态。" : "工具执行完成。生成的文件可在资料与产出中查看。" }] }] } } };
  }
  return null;
}
function text(content) { return (content ?? []).filter((part) => part.type === "text").map((part) => ({ type: "text", text: part.text })); }
