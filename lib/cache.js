const redis = require('redis')
const { promisify } = require('util')
const zlib = require('zlib')
const deflate = promisify(zlib.deflate).bind(zlib)
const inflate = promisify(zlib.inflate).bind(zlib)

class RedisObjectCache {

  constructor(client) {
    this.client = client
    // get, set, del
    this.redisGet = promisify(client.get).bind(client)
    this.redisSet = promisify(client.set).bind(client)
    this.redisDel = promisify(client.del).bind(client)
    // hash
    this.redisHGet = promisify(client.hget).bind(client)
    this.redisHGetAll = promisify(client.hgetall).bind(client)
    this.redisHSet = promisify(client.hset).bind(client)
    this.redisHDel = promisify(client.hdel).bind(client)
    this.redisHLen = promisify(client.hlen).bind(client)
    // expiration
    this.redisExpire = promisify(client.expire).bind(client)
    this.redisTTL = promisify(client.ttl).bind(client)
  }
  
  getClient() {
    return this.client
  }

  isConnected() {
    return this.client && this.client.connected || false
  }

  async quit() {
    return await RedisObjectCache.quitClient(this.client)
  }

  async get(key, field) {
    return await this.decode(await this.redisGet(key))
  }

  async hget(key, field) {
    return await this.decode(await this.redisHGet(key, field))
  }

  async hgetall(key) {
    let hash = await this.redisHGetAll(key)
    for (let field in hash) {
      hash[field] = await this.decode(hash[field])
    }
    return hash
  } 
  
  async hlen(key) {
    return await this.redisHLen(key)
  }

  async set(key, value, ttl) { // ttl in seconds
    let compressed64 = await this.encode(value)
    let args = [ key, compressed64 ]
    if (ttl) {
      args = args.concat('EX', ttl)
    }
    return await this.redisSet(...args)
  }

  async expire(key, ttl) {
    return await this.redisExpire(key, ttl)
  }

  // The command returns -2 if the key does not exist.
  // The command returns -1 if the key exists but has no associated expire.
  async ttl(key) {
    return await this.redisTTL(key)
  }

  async hset(key, field, value) { // individual hash fields do not have ttl
    return await this.redisHSet(key, field, await this.encode(value))
  }
  
  async del(key) {
    return await this.redisDel(key)
  }

  async hdel(key, field) {
    return await this.redisHDel(key, field)
  }

  async decode(value) {
    return value && JSON.parse(await inflate(Buffer.from(value, 'base64'))) || null
  }

  async encode(value) {
    return value && (await deflate(JSON.stringify(value))).toString('base64') || null
  }

  // STATIC

  static async create(url, options) {
    let client = await RedisObjectCache.createClient(url, options)
    return new RedisObjectCache(client)
  }

  static async createClient(url, options) {
    options = options || { }
    let client = redis.createClient(url, options)
    return new Promise((resolve, reject) => {
      client.on("connect", () => {
        resolve(client)
        return
      })
      client.on("error", (err) => {
        reject(err)
        return
      }) 
    })
  }

  static async quitClient(client) {
    return new Promise((resolve, reject) => {
      client.on("end", () => {
        resolve(true)
        return
      })
      client.on("error", (err) => {
        console.error(err)
        reject(err)
        return
      })
      client.quit()
    })
  }
}

module.exports = RedisObjectCache

