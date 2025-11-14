Install dependencies using

```bash
npm i
```

Run tests using `test.sh` script which brings up Postgres on port 15432 (from .env).

NOTE: Tests are run in `watch` mode, so they restart on file changes. Under the hood, they are run using `npm run test -- --watch`.

```bash
./test.sh
```

Modified compiled version of `pg-boss` is in `lib/pg-boss/dist/index.mjs`.
Updated code starts on line 1657.
