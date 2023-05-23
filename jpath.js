
function err (msg) {
    throw Error(msg)
}

function keyfor (s) {
    return /^\d+$/.test(s) ? parseInt(s) : s
}

function check_parent_type (parent, k, path) {
    const expect = (typeof k === 'number') ? 'array' : 'object'
    typeof parent === 'object' || err(`expected parent of ${k} in "${path.join('/')}" to be type ${expect}, but got ${typeof parent} (${parent})`)
    if (expect === 'array') {
        Array.isArray(parent) || err(`expected parent of ${k} in "${path.join('/')}" to be type array, but got object`)
    } else {
        !Array.isArray(parent) || err(`expected parent of ${k} in "${path.join('/')}" to be type object, but got array`)
    }
}

function set_value (parent, path, val) {
    path = path.split('/')
    // ensure we start with a valid parent
    let k = keyfor(path[0])
    if (parent == null) {
        parent = typeof k === 'number' ? [] : {}
    } else {
        check_parent_type(parent, k, path)
    }

    // walk and set parent-> child, creating containers as needed
    let cur = parent
    for (let i=0; i<path.length-1; i++) {
        let nk = keyfor(path[i+1])
        if (cur[k] == null) {
            cur[k] = (typeof nk === 'number' ? [] : {})
        } else {
          check_parent_type(cur[k], nk, path)
        }
        cur = cur[k]
        k = nk
    }
    cur[k] = val
    return parent
}


module.exports = {
    set_value: set_value,
}
