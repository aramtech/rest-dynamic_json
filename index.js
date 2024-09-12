function generate_validator(update) {
    const validator = {
        set: function (_target, key, value) {
            if (typeof value == "object" && !Array.isArray(value)) {
                _target[key] = new Proxy({}, validator);
                for (const sub_key in value) {
                    this.set(_target[key], sub_key, value[sub_key]);
                }
            } else if (typeof value == "object" && Array.isArray(value)) {
                _target[key] = value;
                _target[key]._push = _target[key].push;
                _target[key].push = function () {
                    this._push(...arguments);
                    update();
                };
            } else {
                _target[key] = value;
            }
            update();
            return true;
        },
        deleteProperty(_target, key) {
            delete _target[key];
            update();
            return true;
        },
    };
    return validator;
}

function reload_proxy(proxy, source) {
    for (const key in source) {
        if (typeof source[key] == "object" && !Array.isArray(source[key])) {
            proxy[key] = {};
            reload_proxy(proxy[key], source[key]);
        } else {
            proxy[key] = source[key];
        }
    }
}

export { generate_validator };
export { reload_proxy };
