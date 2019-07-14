require('dotenv').config()
const RedisObjectCache = require('../lib/cache')

describe('Get and Set', () => {
  const key = "my:key"
  const value = { hello: "world" }
  let cache = null
  beforeEach(async () => {
    cache = await RedisObjectCache.create(process.env.REDIS_URL, { password: process.env.REDIS_PASSWORD })
  })
  afterEach(async () => {
    if (cache && cache.isConnected()) {
      await cache.del(key)
      await cache.quit()
    }
  })
  it ('should create a valid connection to redis', async () => {
    expect(cache.isConnected()).toBeTruthy()
  })
  it ('should return null if key does not exist', async () => {
    let data = await cache.get('BAD-KEY')
    expect(data).toBe(null)
  })
  it ('should set, get, and del an object', async () => {
    let ok = await cache.set(key, value)
    expect(ok).toBe("OK")
    let data = await cache.get(key)
    expect(data).toMatchObject(value)
    let delok = await cache.del(key)
    expect(delok).toBe(1)
  })
}) 


describe('Hash Operations', () => {
  const key = "my:hash:key"
  const field = "my:field"
  const value = { hello: "world" }
  let cache = null
  beforeEach(async () => {
    cache = await RedisObjectCache.create(process.env.REDIS_URL, { password: process.env.REDIS_PASSWORD })
  })
  afterEach(async () => {
    if (cache && cache.isConnected()) {
      await cache.del(key)
      await cache.quit()
    }
  })
  it ('should hset, hget, and del', async () => {
    await cache.hset(key, field, value)
    let data = await cache.hget(key, field)
    expect(data).toMatchObject(value)
    expect(await cache.hdel(key, field)).toBe(1)
    expect(await cache.del(key)).toBe(0) // deleting last field deletes the hash
  })
  it ('should throw if trying to plain get a hash', async () => {
    await cache.hset(key, field, value)
    let error = null
    try {
      await cache.get(key)
    } catch (err) {
      error = err
    }
    expect(error).toBeTruthy()
    expect(error.message).toEqual("WRONGTYPE Operation against a key holding the wrong kind of value")
  })
  it ('should delete an entire redis hash with del', async () => {
    await cache.hset(key, field, value)
    await cache.hset(key, "field2", value)
    let fieldvalue = await cache.hget(key, field)
    expect(fieldvalue).toMatchObject(value)
    await cache.del(key)
    fieldvalue = await cache.hget(key, field)
    expect(fieldvalue).toBe(null)
  })
  it ('should get the entire hash', async () => {
    await cache.hset(key, field, value)
    await cache.hset(key, "field2", value)
    let values = await cache.hgetall(key)
    expect(values).toMatchObject({
      "my:field": value,
      "field2": value
    })
  })
  it ('should get the number of keys in a hash', async () => {
    let n = await cache.hlen(key)
    expect(n).toEqual(0)
    await cache.hset(key, field, value)
    expect(await cache.hlen(key)).toEqual(1)
    await cache.hset(key, "field2", value)
    expect(await cache.hlen(key)).toEqual(2)
  })
})

describe('Expire', () => {
  const key = "my:hash:key"
  const field = "my:field"
  const value = { hello: "world" }
  let cache = null
  beforeEach(async () => {
    cache = await RedisObjectCache.create(process.env.REDIS_URL, { password: process.env.REDIS_PASSWORD })
  })
  afterEach(async () => {
    if (cache && cache.isConnected()) {
      await cache.del(key)
      await cache.quit()
    }
  })
  it ('should set expires on a set', async () => { 
    await cache.set(key, value, 10)
    expect(await cache.ttl(key)).toEqual(10)
    await wait(5)
    expect(await cache.ttl(key)).toEqual(5)
    await cache.expire(key, 10)
    expect(await cache.ttl(key)).toEqual(10)
  }, 15 * 1000)
})

async function wait(seconds) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve(seconds)
    }, seconds * 1000)
  })
}