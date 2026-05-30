/* tslint:disable */
/* eslint-disable */

/**
 * Result of the client-side resolver (RFD 0066 Decision 4), as a
 * JS-friendly struct. `expected_version` is non-zero only when
 * `class == Inconsistent`.
 */
export class ResolvedClassification {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    readonly class: WasmLoadClass;
    /**
     * The witness's claimed version when `class == Inconsistent`;
     * `0` for any other class.
     */
    readonly expectedVersion: bigint;
}

/**
 * JS-friendly discriminant for [`LoadClass`]. Matches the proto
 * `LoadStatus` enum's integer values so TS can pun between the two
 * when it already has the proto value.
 */
export enum WasmLoadClass {
    /**
     * Same wire value as `LoadStatus::UNSPECIFIED`. Only returned when
     * an old relay sends no classification; the resolver treats it as
     * `Unavailable` (fail-closed).
     */
    Unspecified = 0,
    Found = 1,
    Empty = 2,
    Inconsistent = 3,
    Unavailable = 4,
}

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
 */
export function resolveClientClassification(server_class: WasmLoadClass, server_expected_version: bigint, has_local_content: boolean): ResolvedClassification;

/**
 * Hard edit guard (RFD 0066 Decision 3).
 *
 * Wraps [`kutl_sync_protocol::should_allow_editing`] for JS. Returns
 * `true` only for `Found` and `Empty`; every other class — including
 * `Unspecified` — fails closed.
 */
export function shouldAllowEditing(_class: WasmLoadClass, expected_version: bigint): boolean;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_resolvedclassification_free: (a: number, b: number) => void;
    readonly resolveClientClassification: (a: number, b: bigint, c: number) => number;
    readonly resolvedclassification_class: (a: number) => number;
    readonly resolvedclassification_expectedVersion: (a: number) => bigint;
    readonly shouldAllowEditing: (a: number, b: bigint) => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
