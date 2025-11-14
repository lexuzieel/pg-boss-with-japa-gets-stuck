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
  test('test pg-boss', async () => {
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
})
