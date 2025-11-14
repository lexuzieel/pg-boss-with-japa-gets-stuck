import EventEmitter from "node:events";

//#region src/types.d.ts
type JobStates = {
  created: 'created';
  retry: 'retry';
  active: 'active';
  completed: 'completed';
  cancelled: 'cancelled';
  failed: 'failed';
};
type Events = {
  error: 'error';
  warning: 'warning';
  wip: 'wip';
  stopped: 'stopped';
};
interface IDatabase {
  executeSql(text: string, values?: unknown[]): Promise<{
    rows: any[];
  }>;
}
interface DatabaseOptions {
  application_name?: string;
  database?: string;
  user?: string;
  password?: string | (() => string) | (() => Promise<string>);
  host?: string;
  port?: number;
  schema?: string;
  ssl?: any;
  connectionString?: string;
  max?: number;
  db?: IDatabase;
}
interface SchedulingOptions {
  schedule?: boolean;
  clockMonitorIntervalSeconds?: number;
  cronWorkerIntervalSeconds?: number;
  cronMonitorIntervalSeconds?: number;
}
interface MaintenanceOptions {
  supervise?: boolean;
  migrate?: boolean;
  createSchema?: boolean;
  warningSlowQuerySeconds?: number;
  warningQueueSize?: number;
  superviseIntervalSeconds?: number;
  maintenanceIntervalSeconds?: number;
  queueCacheIntervalSeconds?: number;
  monitorIntervalSeconds?: number;
}
interface ConstructorOptions extends DatabaseOptions, SchedulingOptions, MaintenanceOptions {}
interface QueueOptions {
  expireInSeconds?: number;
  retentionSeconds?: number;
  deleteAfterSeconds?: number;
  retryLimit?: number;
  retryDelay?: number;
  retryBackoff?: boolean;
  retryDelayMax?: number;
}
interface JobOptions {
  id?: string;
  priority?: number;
  startAfter?: number | string | Date;
  singletonKey?: string;
  singletonSeconds?: number;
  singletonNextSlot?: boolean;
  keepUntil?: number | string | Date;
}
interface ConnectionOptions {
  db?: IDatabase;
}
type InsertOptions = ConnectionOptions;
type SendOptions = JobOptions & QueueOptions & ConnectionOptions;
type QueuePolicy = 'standard' | 'short' | 'singleton' | 'stately' | 'exclusive';
interface Queue extends QueueOptions {
  name: string;
  policy?: QueuePolicy;
  partition?: boolean;
  deadLetter?: string;
  warningQueueSize?: number;
}
interface QueueResult extends Queue {
  deferredCount: number;
  queuedCount: number;
  activeCount: number;
  totalCount: number;
  table: string;
  createdOn: Date;
  updatedOn: Date;
  singletonsActive: string[] | null;
}
type ScheduleOptions = SendOptions & {
  tz?: string;
  key?: string;
};
interface JobPollingOptions {
  pollingIntervalSeconds?: number;
}
interface JobFetchOptions {
  includeMetadata?: boolean;
  priority?: boolean;
  batchSize?: number;
  ignoreStartAfter?: boolean;
}
type WorkOptions = JobFetchOptions & JobPollingOptions;
type FetchOptions = JobFetchOptions & ConnectionOptions;
interface WorkHandler<ReqData> {
  (job: Job<ReqData>[]): Promise<any>;
}
interface WorkWithMetadataHandler<ReqData> {
  (job: JobWithMetadata<ReqData>[]): Promise<any>;
}
interface Request {
  name: string;
  data?: object;
  options?: SendOptions;
}
interface Schedule {
  name: string;
  key: string;
  cron: string;
  timezone: string;
  data?: object;
  options?: SendOptions;
}
interface Job<T = object> {
  id: string;
  name: string;
  data: T;
  expireInSeconds: number;
}
interface JobWithMetadata<T = object> extends Job<T> {
  priority: number;
  state: 'created' | 'retry' | 'active' | 'completed' | 'cancelled' | 'failed';
  retryLimit: number;
  retryCount: number;
  retryDelay: number;
  retryBackoff: boolean;
  retryDelayMax?: number;
  startAfter: Date;
  startedOn: Date;
  singletonKey: string | null;
  singletonOn: Date | null;
  expireInSeconds: number;
  deleteAfterSeconds: number;
  createdOn: Date;
  completedOn: Date | null;
  keepUntil: Date;
  policy: QueuePolicy;
  deadLetter: string;
  output: object;
}
interface JobInsert<T = object> {
  id?: string;
  data?: T;
  priority?: number;
  retryLimit?: number;
  retryDelay?: number;
  retryBackoff?: boolean;
  retryDelayMax?: number;
  startAfter?: number | string | Date;
  singletonKey?: string;
  singletonSeconds?: number;
  expireInSeconds?: number;
  deleteAfterSeconds?: number;
  retentionSeconds?: number;
}
type WorkerState = 'created' | 'active' | 'stopping' | 'stopped';
interface WipData {
  id: string;
  name: string;
  options: WorkOptions;
  state: WorkerState;
  count: number;
  createdOn: number;
  lastFetchedOn: number | null;
  lastJobStartedOn: number | null;
  lastJobEndedOn: number | null;
  lastJobDuration: number | null;
  lastError: object | null;
  lastErrorOn: number | null;
}
interface StopOptions {
  close?: boolean;
  graceful?: boolean;
  timeout?: number;
  wait?: boolean;
}
interface OffWorkOptions {
  id: string;
}
type UpdateQueueOptions = Omit<Queue, 'name' | 'partition' | 'policy'>;
interface Warning {
  message: string;
  data: object;
}
interface CommandResponse {}
type PgBossEventMap = {
  error: [error: Error];
  warning: [warning: Warning];
  wip: [data: WipData[]];
  stopped: [];
};
//#endregion
//#region src/plans.d.ts
declare const JOB_STATES: Readonly<{
  created: "created";
  retry: "retry";
  active: "active";
  completed: "completed";
  cancelled: "cancelled";
  failed: "failed";
}>;
declare const QUEUE_POLICIES: Readonly<{
  standard: "standard";
  short: "short";
  singleton: "singleton";
  stately: "stately";
  exclusive: "exclusive";
}>;
//#endregion
//#region src/index.d.ts
declare const events: Events;
declare function getConstructionPlans(schema?: string): string;
declare function getMigrationPlans(schema?: string, version?: number): string;
declare function getRollbackPlans(schema?: string, version?: number): string;
declare class PgBoss extends EventEmitter<PgBossEventMap> {
  #private;
  constructor(connectionString: string);
  constructor(options: ConstructorOptions);
  start(): Promise<this>;
  stop(options?: StopOptions): Promise<void>;
  send(request: Request): Promise<string | null>;
  send(name: string, data?: object | null, options?: SendOptions): Promise<string | null>;
  sendAfter(name: string, data: object, options: SendOptions, date: Date): Promise<string | null>;
  sendAfter(name: string, data: object, options: SendOptions, dateString: string): Promise<string | null>;
  sendAfter(name: string, data: object, options: SendOptions, seconds: number): Promise<string | null>;
  sendThrottled(name: string, data: object, options: SendOptions, seconds: number, key?: string): Promise<string | null>;
  sendDebounced(name: string, data: object, options: SendOptions, seconds: number, key?: string): Promise<string | null>;
  insert(name: string, jobs: JobInsert[], options?: InsertOptions): Promise<string[] | null>;
  fetch<T>(name: string, options: FetchOptions & {
    includeMetadata: true;
  }): Promise<JobWithMetadata<T>[]>;
  fetch<T>(name: string, options?: FetchOptions): Promise<Job<T>[]>;
  work<ReqData>(name: string, handler: WorkHandler<ReqData>): Promise<string>;
  work<ReqData>(name: string, options: WorkOptions & {
    includeMetadata: true;
  }, handler: WorkWithMetadataHandler<ReqData>): Promise<string>;
  work<ReqData>(name: string, options: WorkOptions, handler: WorkHandler<ReqData>): Promise<string>;
  offWork(name: string): Promise<void>;
  offWork(options: OffWorkOptions): Promise<void>;
  notifyWorker(workerId: string): void;
  subscribe(event: string, name: string): Promise<void>;
  unsubscribe(event: string, name: string): Promise<void>;
  publish(event: string, data?: object, options?: SendOptions): Promise<void>;
  cancel(name: string, id: string | string[], options?: ConnectionOptions): Promise<CommandResponse>;
  resume(name: string, id: string | string[], options?: ConnectionOptions): Promise<CommandResponse>;
  retry(name: string, id: string | string[], options?: ConnectionOptions): Promise<CommandResponse>;
  deleteJob(name: string, id: string | string[], options?: ConnectionOptions): Promise<CommandResponse>;
  deleteQueuedJobs(name: string): Promise<void>;
  deleteStoredJobs(name: string): Promise<void>;
  deleteAllJobs(name: string): Promise<void>;
  complete(name: string, id: string | string[], data?: object, options?: ConnectionOptions): Promise<CommandResponse>;
  fail(name: string, id: string | string[], data?: object, options?: ConnectionOptions): Promise<CommandResponse>;
  getJobById<T>(name: string, id: string, options?: ConnectionOptions): Promise<JobWithMetadata<T> | null>;
  createQueue(name: string, options?: Omit<Queue, 'name'>): Promise<void>;
  updateQueue(name: string, options?: UpdateQueueOptions): Promise<void>;
  deleteQueue(name: string): Promise<void>;
  getQueues(names?: string[]): Promise<QueueResult[]>;
  getQueue(name: string): Promise<QueueResult | null>;
  getQueueStats(name: string): Promise<QueueResult>;
  supervise(name?: string): Promise<void>;
  isInstalled(): Promise<boolean>;
  schemaVersion(): Promise<number | null>;
  schedule(name: string, cron: string, data?: object, options?: ScheduleOptions): Promise<void>;
  unschedule(name: string, key?: string): Promise<void>;
  getSchedules(name?: string, key?: string): Promise<Schedule[]>;
  getDb(): IDatabase;
}
//#endregion
export { type ConnectionOptions, type ConstructorOptions, type IDatabase as Db, type Events, type FetchOptions, type Job, type JobFetchOptions, type JobInsert, type JobPollingOptions, type JobStates, type JobWithMetadata, type MaintenanceOptions, type OffWorkOptions, PgBoss, type Queue, type QueuePolicy, type QueueResult, type Request, type Schedule, type ScheduleOptions, type SchedulingOptions, type SendOptions, type StopOptions, type WipData, type WorkHandler, type WorkOptions, type WorkWithMetadataHandler, events, getConstructionPlans, getMigrationPlans, getRollbackPlans, QUEUE_POLICIES as policies, JOB_STATES as states };
//# sourceMappingURL=index.d.mts.map