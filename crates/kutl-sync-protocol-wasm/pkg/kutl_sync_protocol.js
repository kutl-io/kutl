/* @ts-self-types="./kutl_sync_protocol.d.ts" */

/**
 * Result of the client-side resolver (RFD 0066 Decision 4), as a
 * JS-friendly struct. `expected_version` is non-zero only when
 * `class == Inconsistent`.
 */
export class ResolvedClassification {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(ResolvedClassification.prototype);
        obj.__wbg_ptr = ptr;
        ResolvedClassificationFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        ResolvedClassificationFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_resolvedclassification_free(ptr, 0);
    }
    /**
     * @returns {WasmLoadClass}
     */
    get class() {
        const ret = wasm.resolvedclassification_class(this.__wbg_ptr);
        return ret;
    }
    /**
     * The witness's claimed version when `class == Inconsistent`;
     * `0` for any other class.
     * @returns {bigint}
     */
    get expectedVersion() {
        const ret = wasm.resolvedclassification_expectedVersion(this.__wbg_ptr);
        return BigInt.asUintN(64, ret);
    }
}
if (Symbol.dispose) ResolvedClassification.prototype[Symbol.dispose] = ResolvedClassification.prototype.free;

/**
 * JS-friendly discriminant for [`LoadClass`]. Matches the proto
 * `LoadStatus` enum's integer values so TS can pun between the two
 * when it already has the proto value.
 * @enum {0 | 1 | 2 | 3 | 4}
 */
export const WasmLoadClass = Object.freeze({
    /**
     * Same wire value as `LoadStatus::UNSPECIFIED`. Only returned when
     * an old relay sends no classification; the resolver treats it as
     * `Unavailable` (fail-closed).
     */
    Unspecified: 0, "0": "Unspecified",
    Found: 1, "1": "Found",
    Empty: 2, "2": "Empty",
    Inconsistent: 3, "3": "Inconsistent",
    Unavailable: 4, "4": "Unavailable",
});

/**
 * Client-side symmetric fail-closed rule (RFD 0066 Decision 4).
 *
 * Wraps [`kutl_sync_protocol::resolve_client_classification`] for
 * JS callers. See the native docs for rule details.
 *
 * `server_class` and `server_expected_version` together encode the
 * server's `SubscribeStatus` payload. `has_local_content` is the
 * caller's answer to "does the in-memory adapter for this doc have
 * anything in it?".
 * @param {WasmLoadClass} server_class
 * @param {bigint} server_expected_version
 * @param {boolean} has_local_content
 * @returns {ResolvedClassification}
 */
export function resolveClientClassification(server_class, server_expected_version, has_local_content) {
    const ret = wasm.resolveClientClassification(server_class, server_expected_version, has_local_content);
    return ResolvedClassification.__wrap(ret);
}

/**
 * Hard edit guard (RFD 0066 Decision 3).
 *
 * Wraps [`kutl_sync_protocol::should_allow_editing`] for JS. Returns
 * `true` only for `Found` and `Empty`; every other class — including
 * `Unspecified` — fails closed.
 * @param {WasmLoadClass} _class
 * @param {bigint} expected_version
 * @returns {boolean}
 */
export function shouldAllowEditing(_class, expected_version) {
    const ret = wasm.shouldAllowEditing(_class, expected_version);
    return ret !== 0;
}

function __wbg_get_imports() {
    const import0 = {
        __proto__: null,
        __wbg___wbindgen_throw_6ddd609b62940d55: function(arg0, arg1) {
            throw new Error(getStringFromWasm0(arg0, arg1));
        },
        __wbindgen_init_externref_table: function() {
            const table = wasm.__wbindgen_externrefs;
            const offset = table.grow(4);
            table.set(0, undefined);
            table.set(offset + 0, undefined);
            table.set(offset + 1, null);
            table.set(offset + 2, true);
            table.set(offset + 3, false);
        },
    };
    return {
        __proto__: null,
        "./kutl_sync_protocol_bg.js": import0,
    };
}

const ResolvedClassificationFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_resolvedclassification_free(ptr >>> 0, 1));

function getStringFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return decodeText(ptr, len);
}

let cachedUint8ArrayMemory0 = null;
function getUint8ArrayMemory0() {
    if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
        cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8ArrayMemory0;
}

let cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
cachedTextDecoder.decode();
const MAX_SAFARI_DECODE_BYTES = 2146435072;
let numBytesDecoded = 0;
function decodeText(ptr, len) {
    numBytesDecoded += len;
    if (numBytesDecoded >= MAX_SAFARI_DECODE_BYTES) {
        cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
        cachedTextDecoder.decode();
        numBytesDecoded = len;
    }
    return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}

let wasmModule, wasm;
function __wbg_finalize_init(instance, module) {
    wasm = instance.exports;
    wasmModule = module;
    cachedUint8ArrayMemory0 = null;
    wasm.__wbindgen_start();
    return wasm;
}

async function __wbg_load(module, imports) {
    if (typeof Response === 'function' && module instanceof Response) {
        if (typeof WebAssembly.instantiateStreaming === 'function') {
            try {
                return await WebAssembly.instantiateStreaming(module, imports);
            } catch (e) {
                const validResponse = module.ok && expectedResponseType(module.type);

                if (validResponse && module.headers.get('Content-Type') !== 'application/wasm') {
                    console.warn("`WebAssembly.instantiateStreaming` failed because your server does not serve Wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n", e);

                } else { throw e; }
            }
        }

        const bytes = await module.arrayBuffer();
        return await WebAssembly.instantiate(bytes, imports);
    } else {
        const instance = await WebAssembly.instantiate(module, imports);

        if (instance instanceof WebAssembly.Instance) {
            return { instance, module };
        } else {
            return instance;
        }
    }

    function expectedResponseType(type) {
        switch (type) {
            case 'basic': case 'cors': case 'default': return true;
        }
        return false;
    }
}

function initSync(module) {
    if (wasm !== undefined) return wasm;


    if (module !== undefined) {
        if (Object.getPrototypeOf(module) === Object.prototype) {
            ({module} = module)
        } else {
            console.warn('using deprecated parameters for `initSync()`; pass a single object instead')
        }
    }

    const imports = __wbg_get_imports();
    if (!(module instanceof WebAssembly.Module)) {
        module = new WebAssembly.Module(module);
    }
    const instance = new WebAssembly.Instance(module, imports);
    return __wbg_finalize_init(instance, module);
}

async function __wbg_init(module_or_path) {
    if (wasm !== undefined) return wasm;


    if (module_or_path !== undefined) {
        if (Object.getPrototypeOf(module_or_path) === Object.prototype) {
            ({module_or_path} = module_or_path)
        } else {
            console.warn('using deprecated parameters for the initialization function; pass a single object instead')
        }
    }

    if (module_or_path === undefined) {
        module_or_path = new URL('kutl_sync_protocol_bg.wasm', import.meta.url);
    }
    const imports = __wbg_get_imports();

    if (typeof module_or_path === 'string' || (typeof Request === 'function' && module_or_path instanceof Request) || (typeof URL === 'function' && module_or_path instanceof URL)) {
        module_or_path = fetch(module_or_path);
    }

    const { instance, module } = await __wbg_load(await module_or_path, imports);

    return __wbg_finalize_init(instance, module);
}

export { initSync, __wbg_init as default };
