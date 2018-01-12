/* global describe, it, before, after, emit */

const assert = require('assert')
const CubeSatDB = require('.')
const rimraf = require('rimraf')
const {name, version} = require('./package.json')

before(async function () {
  rimraf.sync('test')
  rimraf.sync('test-*')
  this.cube = new CubeSatDB('test')
  await new Promise((resolve, reject) => {
    this.cube.ipfs.on('error', reject)
    this.cube.ipfs.once('ready', resolve)
  })
})

after(async function () {
  await new Promise((resolve) => {
    this.cube.ipfs.stop(resolve)
  })
  rimraf.sync('test')
  rimraf.sync('test-*')
  // IPFS doesn't close nicely so
  // we have to do things the angry way
  process.exit(0)
})

describe(`${name} ${version}`, function () {
  it('should exist', function () {
    assert(this.cube)
  })

  it('should handle CRUD', async function () {
    let doc1 = { a: 'b' }
    assert.equal(this.cube.log.values.length, 0)
    // test post
    await this.cube.post(doc1)
    assert.equal(this.cube.log.values.length, 1)
    // test alldocs
    let allDocs = await this.cube.all()
    assert.equal(allDocs.length, 1)
    // test get
    let _id = allDocs[0]._id
    let doc2 = await this.cube.get(_id)
    assert.equal(doc1.a, doc2.a)
    // test put
    let doc3 = Object.assign({}, doc2, { c: 'd' })
    await this.cube.put(doc3)
    let doc4 = await this.cube.get(_id)
    assert.equal(doc3.c, doc4.c)
    // test deletion
    await this.cube.del(doc4)
    // assert that the oplog and the pouch are distinct
    assert.equal(this.cube.log.values.length, 3)
    let allDocs2 = await this.cube.all()
    assert.equal(allDocs2.length, 0)
  })

  it('should handle queries', async function () {
    await this.cube.post([{
      name: 'Mario',
      team: 'Mushroom'
    }, {
      name: 'Luigi',
      team: 'Mushroom'
    }, {
      name: 'Bowser',
      team: 'Koopa'
    }])
    // try mango
    let query1 = await this.cube.find({
      selector: {
        team: {
          '$eq': 'Mushroom'
        }
      },
      include_docs: true
    })
    assert.equal(query1.length, 2)
    query1.forEach((doc) => {
      assert.equal(doc.team, 'Mushroom')
    })
    // try ad-hoc mapreduce
    let query2 = await this.cube.query(function (doc) {
      if (doc.team === 'Mushroom') emit(doc._id)
    }, { include_docs: true })
    let docs2 = query2.rows.map(function (row) {
      return row.doc
    })
    assert.equal(query1.length, docs2.length)
    // try design docs
    await this.cube.put({
      _id: '_design/test',
      views: {
        mushroom: {
          map: function (doc) {
            if (doc.team === 'Mushroom') emit(doc._id)
          }.toString(),
          reduce: '_count'
        }
      }
    })
    let query3 = await this.cube.query('test/mushroom')
    assert.equal(query1.length, query3.rows[0].value)
  })

  it('should join two log stores', function () {
    // starting two nodes is a no-no so we pass the one
    // we already have
    let cube = new CubeSatDB('test-copy', {
      ipfs: this.cube.ipfs
    })
    return cube.all()
      .then((docs) => {
        assert.equal(docs.length, 0)
        return cube.join(this.cube)
      })
      .then(async () => {
        let result = await this.cube.all()
        // verify via zipper
        result.forEach(async (doc) => {
          let other = await cube.get(doc._id)
          assert.equal(doc._id, other._id)
          assert.equal(doc._rev, other._rev)
        })
        // however, their oplogs will still differ
        assert.notEqual(cube.log.length, this.cube.log.length)
      })
      .catch((e) => {
        console.trace(e)
        throw e
      })
  })
})
