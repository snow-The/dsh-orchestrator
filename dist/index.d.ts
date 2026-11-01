export declare const name = "orchestrator";
export declare const inject: string[];
interface Tool {
    name: string;
    description: string;
    parameters: {
        properties: Record<string, unknown>;
        required?: string[];
        type: 'object';
    };
    output?: {
        schema: Record<string, unknown>;
        render: (args: unknown, value: unknown) => {
            type: string;
            text: string;
        }[];
    };
    execute: (args: Record<string, unknown>, exec: unknown) => Promise<unknown>;
    isConcurrencySafe?: () => boolean;
}
interface Ctx {
    tools: {
        register: (tool: Tool) => void;
    };
    get?: (name: string) => unknown;
    inject?: (deps: string[], callback: (scope: unknown) => unknown) => unknown;
}
export interface OrchestratorOptions {
    /** Overrides `$DSH_HOME` and `~/.dsh`. */
    dshHome?: string;
}
export declare function apply(ctx: Ctx, options?: OrchestratorOptions): void;
export {};
