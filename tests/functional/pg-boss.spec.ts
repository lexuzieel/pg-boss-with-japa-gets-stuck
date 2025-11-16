import { test } from '@japa/runner'
import { PgBoss } from '../../lib/pg-boss/dist/index.mjs'
import env from '#start/env'

const makeBoss = () => {
  const boss = new PgBoss({
    connectionString: `postgresql://${env.get('DB_USER')}:${env.get('DB_PASSWORD')}@${env.get('DB_HOST')}:${env.get('DB_PORT')}/${env.get('DB_DATABASE')}`,
  })

  return boss
}

test.group('pg-boss', () => {
  test('should not leave open handles', async () => {
    return

    const boss = makeBoss()

    /**
     * Comment these two lines out and restart the tests,
     * live reload will start working.
     */
    await boss.start()
    await boss.stop({
      timeout: 3000, // timeout also doesn't seem to work
    })
  })

  test('should be able to stop upon reaching timeout', async ({ assert }) => {
    const boss = makeBoss()

    boss.on('error', (error) => {
      console.error('error', error)
    })

    await boss.start()

    let inProgressJobs = 0
    const workFor = 4e3 // work is being done for 4 seconds
    const timeout = 3e3 // jobs should be dropped after 3 seconds

    await boss.work('test-queue', async () => {
      inProgressJobs++
      console.log(`[WORKER] processing job for ${workFor / 1000} seconds`)
      await new Promise((resolve) => setTimeout(resolve, workFor))
      console.log(`[WORKER] job completed`)
      inProgressJobs--
    })

    await boss.createQueue('test-queue')

    await boss.send('test-queue', {
      job: 'TestJob',
      payload: { arg1: 'hello', arg2: 1 },
    })

    /**
     * When graceful is true, jobs should be dropped after timeout.
     * Currently, it looks like work is still being done after timeout,
     * whilst the connection to the database is closed.
     * 
     * Because of this, pg-boss emits error like this:
     * 
     *   message: 'Database connection is not opened (Queue: test-queue, Worker: <id>)',
     *   stack: 'AssertionError [ERR_ASSERTION]: Database connection is not opened\n' +
     *     '    at Manager.assertDb (.../pg-boss/src/manager.ts:668:16)\n' +
     *     '    at Manager.fail (.../pg-boss/src/manager.ts:475:46)\n' +
     *     '    at Worker.onFetch (.../pg-boss/src/manager.ts:180:20)\n' +
     *     '    at async Worker.start (.../pg-boss/src/worker.ts:79:11)',
     */
    await boss.stop({
      graceful: true,
      timeout,
    })

    assert.equal(inProgressJobs, 0, `jobs should be dropped after ${timeout / 1000} seconds`)
  })
})
