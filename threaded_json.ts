import cluster from "cluster";
import { randomInt } from "crypto";
import fs from "fs";

import { lock_method as _lock_method, JSONObject } from "../common/index.js";

const rs = (await import("$/server/utils/common/index.js")).default.Object_manipulation.rs;

export type FilePath = string;

export type JsonUpdateQuery = {
    type: "push" | "unshift" | "set";
    selector: string | Array<string>;
    key: string;
    value: any;
};

export type ThreadedJson<SourceType, JSONDefinition extends any> = {
    get: (selector: string | string[]) => Promise<any>;
    set: (selector: string | string[] | null | undefined, key: string, value: any) => Promise<boolean>;
    push: (array_selector: string | string[], value: any) => Promise<boolean>;
    remove_item_from_array: (array_selector: string | string[], item: any, key?: string) => Promise<boolean>;
    updated_item_in_array: (
        array_selector: string | string[],
        item: any,
        updated_item: any,
        key?: string,
    ) => Promise<boolean>;
    splice: (array_selector: string | string[], value: any) => Promise<boolean>;
    unshift: (selector: string | string[], value: any) => Promise<boolean>;
    set_array: (queries: Array<JsonUpdateQuery>) => Promise<boolean>;
    set_multiple_queries: (queries: Array<JsonUpdateQuery>) => Promise<boolean>;
} & (SourceType extends string
    ? {
          update_json_from_provided: (new_content: JSONDefinition) => Promise<void>;
          request_update: () => Promise<void>;
      }
    : {});

export type OptionsNoBroadCast<SourceType> = {
    lazy?: boolean;
    unique_event_number: SourceType extends string ? string | undefined : string;
    broadcast_on_update: false;
    file_path?: string;
};

export type Options<SourceType> = {
    lazy?: boolean;
    unique_event_number: SourceType extends string ? string | undefined : string;
    broadcast_on_update?: true;
    file_path?: string;
};

const generate_positive_int = () => Math.abs(randomInt(1e10));

export type JSONSourceFilePath = `${string}.js` | `${string}.json`

async function make_threaded_json<
    JSONDefinition extends any,
    SourceType extends JSONSourceFilePath | JSONObject,
    OptionsType extends Options<SourceType> | OptionsNoBroadCast<SourceType>,
>(
    source: SourceType,
    options: OptionsType,
): Promise<
    OptionsType extends OptionsNoBroadCast<SourceType>
        ? ThreadedJson<SourceType, JSONDefinition>
        : SourceType extends string
          ? JSONDefinition & ThreadedJson<SourceType, JSONDefinition>
          : SourceType & ThreadedJson<SourceType, JSONDefinition>
> {
    const unique_event_number = `_${options.unique_event_number || source}`;
    const request_event_name = `_${unique_event_number}`;

    let json: any;
    let fail_count_on_opening_file_source_file = 0;
    const json_file_path: string | null =
        options?.file_path || (typeof source == "string" && source.endsWith(".json") ? source : null);
    if (typeof source == "string") {
        while (true) {
            try {
                if (source.endsWith(".js")) {
                    json = (await import(source)).default;
                } else {
                    json = JSON.parse(fs.readFileSync(source, "utf-8"));
                }
                break;
            } catch (error: any) {
                console.log("Error on JSON", error.message || error.msg || error.name);
                fail_count_on_opening_file_source_file += 1;
                if (fail_count_on_opening_file_source_file > 3) {
                    process.exit(-1);
                }
                continue;
            }
        }
    } else {
        json = source;
    }

    const lock_method = <T extends (...args: any[]) => any>(method: T) =>
        _lock_method(method, {
            lock_name: request_event_name,
            lock_timeout: 5e3,
        });

    json.unique_event_number = unique_event_number;

    function get_filtered() {
        const filtered = {};
        for (const key in json) {
            if (!(typeof json[key] == "function" || key.startsWith("_") || key == "process_listener")) {
                filtered[key] = json[key];
            }
        }
        return filtered;
    }

    json.update = function (query_number) {
        const filtered = get_filtered();
        if (!options.lazy && json_file_path) {
            fs.writeFileSync(json_file_path, JSON.stringify(filtered, null, 4));
        }
        if (options?.broadcast_on_update === undefined || options?.broadcast_on_update === true) {
            for (const worker of Object.values(cluster.workers || {})) {
                worker?.send({
                    request: request_event_name,
                    query_number: query_number,
                    json: filtered,
                    update: true,
                });
            }
        }
    };

    if (cluster.isPrimary) {
        json._update_request = lock_method(function (filtered, query_number) {
            for (const key in filtered) {
                if (typeof json[key] == "function") {
                    continue;
                }
                json[key] = filtered[key];
            }

            if (options.lazy && json_file_path) {
                fs.writeFileSync(json_file_path, JSON.stringify(filtered, null, 4));
            }
            json.update(query_number);
        });

        json._set_direct = lock_method(function (selector, key, value, query_number) {
            const _target = rs(selector, json);

            if (typeof _target == "object" && !!_target && !Array.isArray(_target)) {
                _target[key] = value;
                json.update(query_number);
                return true;
            } else {
                return false;
            }
        });

        json._push_direct = lock_method(function (selector, value, query_number) {
            const _target = rs(selector, json);
            if (typeof _target === "object" && Array.isArray(_target)) {
                _target.push(value);
                json.update(query_number);
                return true;
            } else {
                return false;
            }
        });
        json._unshift_direct = lock_method(function (selector, value, query_number) {
            const _target = rs(selector, json);
            if (typeof _target === "object" && Array.isArray(_target)) {
                _target.unshift(value);
                json.update(query_number);
                return true;
            } else {
                return false;
            }
        });

        json._remove_item_from_array_direct = lock_method(function (selector, item, key, query_number) {
            const _target = rs(selector, json);
            if (typeof _target === "object" && Array.isArray(_target)) {
                const itemIndex = _target.findIndex((ti) => {
                    if (!key) {
                        return ti == item;
                    } else {
                        return ti[key] == item[key];
                    }
                });
                if (itemIndex != -1) {
                    _target.splice(itemIndex, 1);
                    json.update(query_number);
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        });

        json._update_item_in_array_direct = lock_method(function (selector, item, updated_item, key, query_number) {
            const _target = rs(selector, json);
            if (typeof _target === "object" && Array.isArray(_target)) {
                const itemIndex = _target.findIndex((ti) => {
                    if (!key) {
                        return ti === item;
                    } else {
                        return ti[key] == item[key];
                    }
                });
                if (itemIndex != -1) {
                    if (typeof _target[itemIndex] == "object") {
                        _target[itemIndex] = {
                            ..._target[itemIndex],
                            updated_item,
                        };
                    } else {
                        _target[itemIndex] = updated_item;
                    }
                    json.update(query_number);
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        });

        /**
         *
         * @param {Array<JsonUpdateQuery>} queries
         */
        json._array_set_direct = lock_method(function (queries, query_number) {
            for (const query of queries) {
                if (query.type == "push") {
                    const _target = rs(query.selector, json);
                    if (typeof _target === "object" && Array.isArray(_target)) {
                        _target.push(query.value);
                    }
                } else if (query.type == "set") {
                    const _target = rs(query.selector, json);
                    if (typeof _target === "object" && !!_target) {
                        _target[query.key] = query.value;
                    }
                } else if (query.type == "unshift") {
                    const _target = rs(query.selector, json);
                    if (typeof _target === "object" && Array.isArray(_target)) {
                        _target.unshift(query.value);
                    }
                }
            }
            json.update(query_number);
        });

        json._splice_direct = lock_method(function (selector, start_index: number, delete_count: number, query_number) {
            const _target = rs(selector, json);
            if (typeof _target === "object" && Array.isArray(_target)) {
                const result = _target.splice(start_index, delete_count);
                json.update(query_number);
                return result;
            } else {
                return false;
            }
        });

        json._pop_direct = lock_method(function (selector, query_number) {
            const _target = rs(selector, json);
            if (typeof _target === "object" && Array.isArray(_target)) {
                const result = _target.pop();
                json.update(query_number);
                return result;
            } else {
                return false;
            }
        });

        json._shift_direct = lock_method(function (selector, query_number) {
            const _target = rs(selector, json);
            if (typeof _target === "object" && Array.isArray(_target)) {
                const result = _target.shift();
                json.update(query_number);
                return result;
            } else {
                return false;
            }
        });

        json._workers_listeners = [];
        for (const worker of Object.values(cluster.workers || {})) {
            json._workers_listeners.push(
                worker?.on("message", async (msg, socket) => {
                    try {
                        if (msg.request == request_event_name) {
                            if (msg.query?.type == "get") {
                                worker.send({
                                    query_number: msg.query_number,
                                    result: rs(msg.query.selector, json),
                                    finished: true,
                                });
                            } else {
                                let result;
                                if (msg.query?.type == "set") {
                                    result = await json._set_direct(
                                        msg.query.selector,
                                        msg.query.key,
                                        msg.query.value,
                                        msg.query_number,
                                    );
                                } else if (msg.query?.type == "push") {
                                    result = await json._push_direct(
                                        msg.query.selector,
                                        msg.query.value,
                                        msg.query_number,
                                    );
                                } else if (msg.query?.type == "unshift") {
                                    result = await json._unshift_direct(
                                        msg.query.selector,
                                        msg.query.value,
                                        msg.query_number,
                                    );
                                } else if (msg.query?.type == "array_set") {
                                    result = await json._array_set_direct(msg.query.queries, msg.query_number);
                                } else if (msg.query?.type == "update") {
                                    result = await json._update_request(msg.query.value, msg.query_number);
                                } else if (msg.query?.type == "pop") {
                                    result = await json._pop_direct(msg.query.selector, msg.query_number);
                                } else if (msg.query?.type == "remove_item_from_array") {
                                    result = await json._remove_item_from_array_direct(
                                        msg.query.selector,
                                        msg.query.item,
                                        msg.query.key,
                                        msg.query_number,
                                    );
                                } else if (msg.query?.type == "update_item_in_array") {
                                    result = await json._update_item_in_array_direct(
                                        msg.query.selector,
                                        msg.query.item,
                                        msg.query.updated_item,
                                        msg.query.key,
                                        msg.query_number,
                                    );
                                } else if (msg.query?.type == "shift") {
                                    result = await json._shift_direct(msg.query.selector, msg.query_number);
                                } else if (msg.query?.type == "splice") {
                                    result = await json._splice_direct(
                                        msg.query.selector,
                                        msg.query.start_index,
                                        msg.query.delete_count,
                                        msg.query_number,
                                    );
                                }

                                worker.send({
                                    query_number: msg.query_number,
                                    finished: true,
                                    result,
                                });
                            }
                        }
                    } catch (error) {
                        worker.send({
                            query_number: msg.query_number,
                            finished: true,
                            error,
                        });
                    }
                }),
            );
        }
    } else {
        json.process_listener = process.on("message", (msg: any) => {
            if (msg.request == request_event_name && msg.update) {
                for (const key in msg.json) {
                    if (typeof json[key] == "function") {
                        continue;
                    }
                    json[key] = msg.json[key];
                }
            }
        });
    }

    json.get = async function (selector) {
        if (cluster.isPrimary) {
            // console.log("looking for selector", selector, rs(selector, json));
            return rs(selector, json);
        } else {
            const query = {
                type: "get",
                selector: selector,
            };
            return await send_query(query);
        }
    };

    json.update_item_in_array = async function (selector: string, item: any, updated_item: any, key: number) {
        if (cluster.isPrimary) {
            return json._update_item_in_array_direct(selector, item, key);
        } else {
            const query = {
                type: "update_item_in_array",
                selector,
                item,
                updated_item,
                key,
            };
            return await send_query(query);
        }
    };

    json.update_json_from_provided = async function (new_json: any) {
        if (cluster.isPrimary) {
            return json._update_request(new_json);
        } else {
            const query = {
                type: "update",
                value: new_json,
            };
            return await send_query(query);
        }
    };

    json.remove_item_from_array = async function (selector: string, item: any, key: number) {
        if (cluster.isPrimary) {
            return json._remove_item_from_array_direct(selector, item, key);
        } else {
            const query = {
                type: "remove_item_from_array",
                selector,
                item,
                key,
            };
            return await send_query(query);
        }
    };
    json.splice = async function (selector: string, start_index: number, delete_count: number) {
        if (cluster.isPrimary) {
            return json._splice_direct(selector, start_index, delete_count);
        } else {
            const query = {
                type: "splice",
                selector: selector,
                start_index,
                delete_count,
            };
            return await send_query(query);
        }
    };

    json.pop = async function (selector: string) {
        if (cluster.isPrimary) {
            return json._pop_direct(selector);
        } else {
            const query = {
                type: "pop",
                selector: selector,
            };
            return await send_query(query);
        }
    };

    json.shift = async function (selector: string) {
        if (cluster.isPrimary) {
            return json._shift_direct(selector);
        } else {
            const query = {
                type: "shift",
                selector: selector,
            };
            return await send_query(query);
        }
    };

    json.set = async function (selector: string, key: string, value: any) {
        if (cluster.isPrimary) {
            return json._set_direct(selector, key, value);
        } else {
            const query = {
                type: "set",
                selector: selector,
                key: key,
                value: value,
            };
            return await send_query(query);
        }
    };

    json.set = async function (selector: string, key: string, value: any) {
        if (cluster.isPrimary) {
            return json._set_direct(selector, key, value);
        } else {
            const query = {
                type: "set",
                selector: selector,
                key: key,
                value: value,
            };
            return await send_query(query);
        }
    };

    json.push = async function (selector, value) {
        if (cluster.isPrimary) {
            return json._push_direct(selector, value);
        } else {
            const query = {
                type: "push",
                selector: selector,
                value: value,
            };
            return await send_query(query);
        }
    };

    json.unshift = async function (selector, value) {
        if (cluster.isPrimary) {
            return json._unshift_direct(selector, value);
        } else {
            const query = {
                type: "unshift",
                selector: selector,
                value: value,
            };
            return await send_query(query);
        }
    };

    json.set_array = async function (queries: Array<JsonUpdateQuery>) {
        if (cluster.isPrimary) {
            return json._array_set_direct(queries);
        } else {
            const query = {
                type: "array_set",
                queries: queries,
            };
            return await send_query(query);
        }
    };

    json.set_multiple_queries = json.set_array;

    json.request_update = async function () {
        if (cluster.isPrimary) {
            return json.update();
        } else {
            const filtered = get_filtered();
            const query = {
                type: "update",
                value: filtered,
            };
            await send_query(query);
        }
    };

    const send_query = async (query) => {
        const query_number = generate_positive_int();
        process.send?.({
            request: request_event_name,
            query_number: query_number,
            query: query,
        });
        return await wait_for_query_number(query_number);
    };

    const wait_for_query_number = (query_number) => {
        return new Promise((resolve, reject) => {
            const handler = (msg) => {
                if (msg.query_number == query_number && msg.finished) {
                    if (msg.error) {
                        reject(msg.error);
                    } else {
                        resolve(msg.result);
                    }
                    clearTimeout(timer);
                    process.removeListener("message", handler);
                }
            };
            process.addListener("message", handler);
            const timer = setTimeout(() => {
                try {
                    process.removeListener("message", handler);
                    reject("Timeout");
                } catch (error) {
                    console.log(error);
                }
            }, 5e3);
        });
    };

    return json;
}

export default make_threaded_json;
