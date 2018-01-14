'use strict'

const assert = require('assert')
const IPFS = require('ipfs')
const IpfsLog = require('ipfs-log')
const PouchDB = require('pouchdb')
PouchDB.plugin(require('pouchdb-find'))

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
    assert(name, 'CubeSatDB requires a name or an address')
    if (name instanceof Object) {
      this._hash = name.hash
      this._name = name.name
    } else if ((typeof name) === 'string') {
      this._name = name
    } else {
      throw new CubeError(`Unrecognized type of name: ${typeof name} ${JSON.stringify(name)}`)
    }
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
    this._log = new this._options.IpfsLog(this.ipfs, this.name)
    this._pouch = new this._options.PouchDB(this.name, this._options.pouch)
  }

  /**
   * Add or update a document.
   *
   * If you pass an array of docs,
   * it will pass each of them to this method
   * and resolve once they all resolve.
   *
   * @param  {Object|Array} doc   The document to save, or an array of such documents.
   * @param  {String}  doc._id    The document's ID. Required for `.put()`
   * @return {Promise}
   */
  put (doc) {
    if (doc instanceof Array) {
      const tasks = doc.map((doc) => {
        return this.put(doc)
      })
      return Promise.all(tasks)
    }
    this.validate(doc)
    return this.pouch.put(doc).then((result) => {
      doc._rev = result.rev
      return this.log.append(doc)
    })
  }

  /**
   * Post a document to the store, creating an ID for it.
   * To add a document with an ID, see `.put()`.
   *
   * If you pass an array of docs,
   * it will pass each of them to this method
   * and resolve once they all resolve.
   *
   * @param  {Object|Array} doc   The document to save, or an array of such documents.
   * @return {Promise}
   */
  post (doc) {
    if (doc instanceof Array) {
      const tasks = doc.map((doc) => {
        return this.post(doc)
      })
      return Promise.all(tasks)
    }
    this.validate(doc)
    // post doc
    return this.pouch.post(doc).then((result) => {
      // apply new properties
      doc._id = result.id
      doc._rev = result.rev
      // apply doc to log
      return this.log.append(doc)
    })
  }

  /**
   * Retrieve a document. Wrapper around [PouchDB#get()](https://pouchdb.com/api.html#fetch_document)
   * @return {Promise<Object>} A promise that resolves to the specified document.
   */
  get () {
    return this.pouch.get.apply(this.pouch, arguments)
  }

  /**
   * Retrieve all documents. Wrapper around [PouchDB#allDocs](https://pouchdb.com/api.html#batch_fetch)
   *
   * @param {Object} options
   * @return {Promise<Array<Object>>} Resolves to an array of retrieved documents.
   */
  all (options = {}) {
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
   * Delete a document using its ID and revision value.
   *
   * @param  {Object} doc      The document to delete.
   * @param  {String} doc._id  The document's ID. Required.
   * @param  {String} doc._rev The document's revision value. Required.
   * @return {Promise}
   */
  del ({ _id, _rev }) {
    assert(_id, 'doc requires an _id to delete it.')
    assert(_rev, 'doc requires a _rev to delete it.')
    // remove from local
    return this.pouch.remove(_id, _rev).then((result) => {
      // add op to log
      return this.log.append({
        _id,
        _rev: result.rev,
        _deleted: true
      })
    })
  }

  /**
   * Wrapper around [PouchDB#find](https://pouchdb.com/api.html#query_index) that returns each selected document.
   * @return {Promise<Array<Object>>} Selected documents.
   */
  find () {
    return this.pouch.find.apply(this.pouch, arguments).then((result) => {
      return result.docs
    })
  }

  /**
   * Alias to [PouchDB#query()](https://pouchdb.com/api.html#query_database).
   * @return {Promise<Object>} Query result.
   */
  query () {
    return this.pouch.query.apply(this.pouch, arguments)
  }

  /**
   * Merges another CubeSatDB or IpfsLog into this one.
   * @param  {IpfsLog|CubeSatDB} log An instance of IpfsLog or CubeSatDB.
   * @return {Promise} [description]
   */
  join (log) {
    if (log.log) log = log.log
    return this.log.join(log).then(() => {
      // apply log.values to this.pouch to catch up
      let tasks = log.values.map((entry) => {
        return this.pouch.bulkDocs({
          docs: [entry.payload],
          new_edits: false
        })
        .catch((e) => {
          console.log(e)
        })
      })
      return Promise.all(tasks)
    })
  }

  /**
   * Loads the oplog from the store's IPFS hash.
   *
   * Errors out if the store wasnt constructed with a hash
   * or hasn't established one yet using `.toMultihash()`.
   *
   * @return {Promise} A promise that resolves once the store has loaded.
   */
  load () {
    let hash = this.hash
    return this._options.IpfsLog
      .fromMultihash(this.ipfs, hash)
      .then((log) => {
        return this.join(log)
      })
  }

  /**
   * A document validator. It makes sure a document is
   * an object but not an array.
   *
   * Subclasses can extend this method to enforce a schema.
   *
   * @param  {Object} doc A document.
   * @throws {CubeError}   An error about the document.
   * @todo  expand using https://wiki.apache.org/couchdb/HTTP_Document_API#Special_Fields
   */
  validate (doc) {
    if (!(doc instanceof Object)) throw new CubeError('Document must be an object.')
    if (doc instanceof Array) throw new CubeError('Document must not be an array.')
  }

  /**
   * @return {Promise<String>} The multihash for this store. Use this value to clone the store.
   */
  toMultihash () {
    return this.log.toMultihash().then((hash) => {
      this._hash = hash
      return hash
    })
  }

  /**
   * @return {String} The multihash for the store.
   * @throws {CubeError} If `CubeSatDB#toMultihash()` has not been called yet.
   */
  get hash () {
    if (!this._hash) throw new CubeError('DB does not have a hash yet. Call .toMultihash() first.')
    return this._hash
  }

  /**
   * @return {String} The name of this database.
   */
  get name () {
    return this._name
  }

  /**
   * @return {PouchDB} The CubeSatDB instance's instance of PouchDB.
   */
  get pouch () {
    return this._pouch
  }

  /**
   * @return {IpfsLog} The CubeSatDB instance's instance of IpfsLog.
   */
  get log () {
    return this._log
  }

  /**
   * @return {IPFS} The CubeSatDB instance's instance of an IPFS node.
   */
  get ipfs () {
    return this._ipfs
  }
}

module.exports = CubeSatDB
