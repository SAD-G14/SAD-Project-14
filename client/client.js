import { python, PyClass } from 'pythonia'
const pyClient = await python('./client.py')

class Client extends PyClass {
	constructor(host, port) {
		super(pyClient.Client, null, { host: host, port: port })
	}

	push_js(key, value) {
		return this.push(key, value)
	}

	pull_js() {
		return this.pull()
	}

	subscribe_js(f) {
		return this.subscribe(f)
	}
}

const client = Client.init()
console.log(client.push(3, 3))
python.exit()