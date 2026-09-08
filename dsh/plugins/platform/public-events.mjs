/** Product events omit prompt snapshots, provider configuration and internal model requests. */
export function publicEvent(event) {
  const { type, seq, time, data } = event;
  if (type === "user/message") {
    if (data?.source?.kind !== "user") return null;
    return { type, seq, time, data: { content: textContent(data.content) } };
  }
  if (type === "assistant/message")
    return {
      type,
      seq,
      time,
      data: {
        turn: data.turn,
        step: data.step,
        message: { content: textContent(data.message?.content) },
      },
    };
  if (type === "assistant/chunk" && data?.chunk?.type === "text-delta")
    return {
      type,
      seq,
      time,
      data: {
        turn: data.turn,
        step: data.step,
        chunk: { type: "text-delta", text: data.chunk.text },
      },
    };
  if (["turn/start", "turn/end"].includes(type))
    return { type, seq, time, data };
  if (type === "tool/call")
    return { type, seq, time, data: { name: data.name, callId: data.callId } };
  if (type === "tool/result")
    return {
      type,
      seq,
      time,
      data: { callId: data.message?.toolCallId, error: Boolean(data.error) },
    };
  if (type.startsWith("agent-teams/")) return { type, seq, time, data };
  return null;
}
function textContent(content) {
  return (content ?? [])
    .filter((c) => c.type === "text")
    .map((c) => ({ type: "text", text: c.text }));
}
