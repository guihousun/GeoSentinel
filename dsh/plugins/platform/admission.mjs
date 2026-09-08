import { PlatformError } from "./store.mjs";

export class ResearchAdmission {
  constructor(isActive, maxActive = 8) {
    this.isActive = isActive;
    this.maxActive = maxActive;
    this.slots = new Map();
    this.tail = Promise.resolve();
  }
  async claim(userId, chatId) {
    const check = async () => {
      for (const [id, slot] of this.slots)
        if (Date.now() - slot.at > 1000 && !(await this.isActive(id)))
          this.slots.delete(id);
      if (
        [...this.slots.values()].some(
          (slot) => slot.userId === userId && slot.chatId !== chatId,
        )
      )
        throw new PlatformError(
          429,
          "当前账号已有正在执行的研究对话，请等待或停止后重试",
        );
      if (!this.slots.has(chatId) && this.slots.size >= this.maxActive)
        throw new PlatformError(429, "平台研究任务已达到并发上限，请稍后重试");
      this.slots.set(chatId, { userId, chatId, at: Date.now() });
    };
    const pending = this.tail.then(check, check);
    this.tail = pending.catch(() => {});
    return pending;
  }
  release(chatId) {
    this.slots.delete(chatId);
  }
}
