/** Durable FIFO admission around AgentTeams; not a second agent loop. */
export class ResearchQueue {
  constructor(ledger, { dispatch, isActive, recover, clock = Date.now }) {
    Object.assign(this, { ledger, dispatch, isActive, recover, clock });
    this.dispatching = new Set(); this.pending = new Set(); this.closed = false;
  }
  start() { if (this.timer || this.closed) return; this.timer = setInterval(() => void this.tick(), 500); this.timer.unref(); void this.tick(); }
  async close() {
    this.closed = true; clearInterval(this.timer);
    while (this.busy) await new Promise((resolve) => setTimeout(resolve, 10));
    await Promise.allSettled([...this.pending]);
  }
  async tick() {
    if (this.busy || this.closed) return;
    this.busy = true;
    try {
      for (const job of this.ledger.needsRecovery("research")) {
        if (this.closed) return;
        await this.recover(job); this.ledger.recovered(job.id);
      }
      for (const job of this.ledger.running("research")) {
        if (this.closed) return;
        if (!this.dispatching.has(job.id) && this.clock() - job.started >= 1500 && !(await this.isActive(job.chat_id)))
          this.ledger.finish(job.id, job.status === "cancelling" ? "cancelled" : "completed");
      }
      const seen = new Set();
      for (const job of this.ledger.queued("research")) {
        if (this.closed) return;
        if (seen.has(job.chat_id)) continue;
        seen.add(job.chat_id);
        if (!this.ledger.start(job.id)) continue;
        this.dispatching.add(job.id);
        const work = Promise.resolve().then(() => { if (!this.closed) return this.dispatch(job); }).catch((error) => {
          this.ledger.finish(job.id, "failed", error.status ? error.message : "任务启动失败，请查看服务日志");
          console.error("Research dispatch:", error.message);
        }).finally(() => { this.dispatching.delete(job.id); this.pending.delete(work); });
        this.pending.add(work);
      }
      this.error = null;
    } catch (error) {
      this.error = "任务恢复尚未完成，队列暂缓调度";
      if (this.lastError !== error.message) console.error("Research queue:", error.message);
      this.lastError = error.message;
    } finally { this.busy = false; }
  }
}
