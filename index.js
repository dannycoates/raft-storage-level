var level = require('level')

function LevelStorage(options) {
	options = options || {}
	this.db = level(options.db || './raft.db', { keyEncoding: 'binary', valueEncoding: 'json'})
	this.length = 0
}

LevelStorage.prototype.load = function (callback) {
	var state = {
		currentTerm: 0,
		votedFor: 0,
		entries: []
	}
	this.db.createReadStream()
		.on('data', function (data) {
			var buf = Buffer(data.key, 'binary')
			if(buf.length > 4) {
				state[buf.toString()] = data.value
			}
			else {
				state.entries[buf.readUInt32BE(0)] = data.value
			}
		})
		.on('error', function (err) {
			if (callback) { callback(err) }
			callback = null
		})
		.on('end', function () {
			if (!callback) { return }
			this.length = state.entries.length
			callback(null, state)
		}.bind(this))
}

function add(batch, object) {
	var keys = Object.keys(object)
	for (var i = 0; i < keys.length; i++) {
		var key = keys[i]
		batch.put(key, object[key], { sync: true })
	}
}

function itob(index) {
	var b = Buffer(4)
	b.writeUInt32BE(index, 0)
	return b
}

LevelStorage.prototype.appendEntries = function (startIndex, entries, state, callback) {
	var batch = this.db.batch()
	add(batch, state)
	for (var i = 0, x = startIndex; i < entries.length; i++, x++) {
		batch.put(itob(x), entries[i], { sync: true })
	}
	var nextIndex = startIndex + entries.length
	if (this.length > nextIndex) {
		for (var x = nextIndex; x < this.length; x++) {
			batch.del(itob(x), { sync: true })
		}
	}
	this.length = nextIndex
	batch.write(callback)
}

LevelStorage.prototype.set = function (state, callback) {
	var batch = this.db.batch()
	add(batch, state)
	batch.write(callback)
}

module.exports = LevelStorage
