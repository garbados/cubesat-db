'use strict'

const assert = require('assert')
const IPFS = require('ipfs')
const IpfsLog = require('ipfs-log')
const PouchDB = require('pouchdb')
PouchDB.plugin(require('pouchdb-find'))

const OPS = { add: 'ADD', del: 'DEL' }

class CubeError extends Error {}

/**
 * @param {String}      name              A name, URL (of a CouchDB instance), or multihash (of an IPFS log)
 * @param {Object}      options           An object of settings and configuration values.
 * @param {IPFS|Object} options.ipfs      [description]
 * @param {Object}      options.pouch     [description]
 * @param {IpfsLog}     options._IpfsLog  [description]
 * @param {IPFS}        options._IPFS     [description]
 * @param {PouchDB}     options._PouchDB  [description]
 */
class CubeSatDB {
  /**
   * A subclass of Error for describing cube-related problems.
   *
   * @returns {CubeError}
   */
  static get Error () {
    return CubeError
  }

  constructor (name, options = {}) {
    assert(name, 'CubeSatDB requires ')
    this._options = {
      ipfs: options.ipfs || {},
      pouch: options.pouch || {},
      IPFS: options.IPFS || IPFS,
      PouchDB: options.PouchDB || PouchDB,
      IpfsLog: options.IpfsLog || IpfsLog
    }
    let ipfs = this._options.ipfs
    if (ipfs instanceof IPFS) {
      this._ipfs = this._options.ipfs
    } else if (ipfs instanceof Object) {
      this._ipfs = new this._options.IPFS(ipfs)
    } else {
      this._ipfs = new this._options.IPFS()
    }
    // TODO use interpret name as hash?
    this._log = new this._options.IpfsLog(this.ipfs, name)
    this._pouch = new this._options.PouchDB(name, this._options.pouch)
  }

  /**
   * A document validator. It makes sure a document is
   * an object but not an array.
   *
   * Subclasses can extend this method to enforce a schema.
   *
   * @param  {Object} doc A document.
   * @throws {CubeError}   An error about the document.
   */
  validate (doc) {
    if (!(doc instanceof Object)) throw new CubeError('Document must be an object.')
    if (doc instanceof Array) throw new CubeError('Document must not be an array.')
  }

  /**
   * Merges another CubeSatDB or IpfsLog into this one.
   * @param  {IpfsLog|CubeSatDB} log An instance of IpfsLog or CubeSatDB.
   */
  async join (log) {
    if (log.log) log = log.log
    await this.log.join(log)
    // apply this.log.values to this.pouch to catch up
    for (let i = 0; i < log.length; i++) {
      let entry = log.values[i]
      let doc = entry.payload.doc
      await this.pouch.bulkDocs({
        docs: [doc],
        new_edits: false
      })
    }
  }

  /**
   * [put description]
   * @param  {[type]} doc [description]
   * @return {[type]}     [description]
   */
  async put (doc) {
    if (doc instanceof Array) {
      // FIXME cheap bulkDocs hack whoops
      return doc.map(async (doc) => {
        let result = await this.put(doc)
        return result
      })
    }
    this.validate(doc)
    let op = OPS.add
    let result = await this.pouch.put(doc)
    doc._rev = result.rev
    await this.log.append({ op, doc })
  }

  /**
   * [post description]
   * @param  {[type]} doc [description]
   * @return {[type]}     [description]
   */
  async post (doc) {
    if (doc instanceof Array) {
      // FIXME cheap bulkDocs hack whoops
      return doc.forEach(async (doc) => {
        await this.post(doc)
      })
    }
    this.validate(doc)
    // post doc
    let result = await this.pouch.post(doc)
    // apply new properties
    doc._id = result.id
    doc._rev = result.rev
    // apply doc to log
    let op = OPS.add
    await this.log.append({ op, doc })
  }

  /**
   * [get description]
   * @return {[type]} [description]
   */
  async get () {
    let result = await this.pouch.get.apply(this.pouch, arguments)
    return result
  }

  /**
   * [all description]
   * @return {[type]} [description]
   */
  async all (options = {}) {
    // default to including docs
    options.include_docs = options.include_docs || true
    return this.pouch.allDocs(options).then(function (result) {
      if (options.include_docs) {
        // format output to resemble find()
        return result.rows.map(function (row) {
          return row.doc
        })
      } else {
        return result.rows
      }
    })
  }

  /**
   * [del description]
   * @param  {String} doc._id  [description]
   * @param  {String} doc._rev [description]
   * @param  {Object} options  [description]
   */
  async del ({ _id, _rev }, options = {}) {
    assert(_id, 'doc requires an _id to delete it.')
    assert(_rev, 'doc requires a _rev to delete it.')
    // remove from local
    let result = await this.pouch.remove(_id, _rev, options)
    // add DEL op to log
    await this.log.append({
      op: OPS.del,
      doc: { _id, _rev: result.rev, _deleted: true }
    })
  }

  /**
   * [find description]
   * @return {[type]} [description]
   */
  async find () {
    let result = await this.pouch.find.apply(this.pouch, arguments)
    return result.docs
  }

  /**
   * [query description]
   * @return {[type]} [description]
   */
  async query () {
    let result = await this.pouch.query.apply(this.pouch, arguments)
    return result
  }

  async toMultihash () {
    let hash = await this.log.toMultihash()
    return hash
  }

  /**
   * [pouch description]
   * @return {PouchDB} The CubeSatDB instance's instance of PouchDB.
   */
  get pouch () {
    return this._pouch
  }

  /**
   * [log description]
   * @return {IpfsLog} The CubeSatDB instance's instance of IpfsLog.
   */
  get log () {
    return this._log
  }

  /**
   * [ipfs description]
   * @return {IPFS} The CubeSatDB instance's instance of an IPFS node.
   */
  get ipfs () {
    return this._ipfs
  }
}

module.exports = CubeSatDB
