import { randomUUID } from "node:crypto";
import { PlatformError } from "./store.mjs";

/** Authenticated transport for DSH's user-questions waterfall, not a second agent loop. */
export class QuestionTransport {
  constructor({ approve, audit = () => {} }) {
    this.approve = approve; this.audit = audit; this.pending = new Map(); this.reviews = new Map();
  }
  wait(user, chatId, questions, signal, review) {
    if (signal?.aborted) return Promise.reject(this.error("ASK_ABORTED"));
    if (this.pending.has(chatId)) throw new PlatformError(409, "当前对话已有待回答问题");
    if (!Array.isArray(questions) || !questions.length || questions.length > 8 || JSON.stringify(questions).length > 48000)
      throw new PlatformError(400, "问题数量或内容超出限制");
    const ids = new Set();
    for (const question of questions) {
      if (typeof question.id !== "string" || ids.has(question.id) || typeof question.question !== "string") throw new PlatformError(400, "问题格式无效");
      ids.add(question.id);
    }
    return new Promise((resolve, reject) => {
      const record = { id: randomUUID(), userId: user.id, chatId, questions: structuredClone(questions), review, resolve, reject, busy: false };
      const abort = () => this.remove(record, this.error("ASK_ABORTED"));
      record.cleanup = () => signal?.removeEventListener("abort", abort);
      this.pending.set(chatId, record); signal?.addEventListener("abort", abort, { once: true });
    });
  }
  error(code) { return Object.assign(new Error(code === "ASK_CANCELLED" ? "用户选择讨论或取消本次提问" : "提问已失效"), { name: "UserQuestionError", code }); }
  remove(record, error, answer) {
    if (this.pending.get(record.chatId) !== record) return;
    this.pending.delete(record.chatId); record.cleanup();
    if (error) record.reject(error); else record.resolve(answer);
  }
  snapshot(user, chatId) {
    const record = this.pending.get(chatId);
    if (!record || record.userId !== user.id) return null;
    return { id: record.id, questions: record.questions, kind: record.review ? "plan-review" : "question" };
  }
  async answer(user, chatId, id, answer, cancel = false) {
    const record = this.pending.get(chatId);
    if (!record || record.id !== id || record.userId !== user.id) throw new PlatformError(409, "问题已失效，请刷新后重试");
    if (record.busy) throw new PlatformError(409, "回答正在提交");
    if (cancel) { this.remove(record, this.error("ASK_CANCELLED")); this.audit(user.id, "question.cancel", id); return; }
    const replies = answer?.answers;
    if (!Array.isArray(replies) || replies.length !== record.questions.length) throw new PlatformError(400, "请完整回答问题");
    const seen = new Set();
    for (const reply of replies) {
      const question = record.questions.find((q) => q.id === reply.id);
      if (!question || seen.has(reply.id) || !Array.isArray(reply.selected) || new Set(reply.selected).size !== reply.selected.length || (!question.multiSelect && reply.selected.length > 1)) throw new PlatformError(400, "回答格式无效");
      seen.add(reply.id);
      if (reply.selected.some((label) => !(question.options ?? []).some((o) => o.label === label))) throw new PlatformError(400, "选项无效");
      if (reply.custom !== undefined && (typeof reply.custom !== "string" || reply.custom.length > 8000)) throw new PlatformError(400, "补充内容无效");
      // The native generic question flow explicitly supports skipping a question.
      if (record.review && !reply.selected.length) throw new PlatformError(400, "请选择方案决定");
    }
    record.busy = true;
    try {
      if (record.review && replies[0].selected.includes(record.review.approve)) {
        await this.approve(user, chatId, record.review.teamId, record.review.revision);
      }
      this.remove(record, null, { answers: replies.map(({ id, selected, custom }) => ({ id, selected, ...(custom === undefined ? {} : { custom }) })) });
      this.audit(user.id, "question.answer", id);
    } catch (error) { record.busy = false; throw error; }
  }
  cancel(chatId) {
    const record = this.pending.get(chatId);
    if (record) this.remove(record, this.error("ASK_ABORTED"));
    this.reviews.delete(chatId);
  }
  close() { for (const chatId of [...this.pending.keys()]) this.cancel(chatId); }
}
