// The node surface `src/smoke.tsx` uses, declared locally.
//
// `@types/node` would be the ordinary answer, and it is one devDependency
// more than this project is allowed (see T87). Declaring the four things the
// smoke render actually touches keeps it inside `tsc`'s reach, which is the
// point: the smoke file is where component props are checked against the
// fixtures, and an unchecked test file is a test file that quietly rots.
declare module "node:fs" {
    export function readFileSync(path: string, encoding: "utf8"): string;
    export function writeFileSync(path: string, data: string): void;
}

declare module "node:path" {
    export function join(...parts: string[]): string;
}

declare const process: {
    env: Record<string, string | undefined>;
    exit(code?: number): never;
};
