type Options = {
    debug: boolean;
    map?: any;
    index: {
        map: Record<string, string>;
    };
    field: {
        zone: {
            name: string;
        };
        base: {
            name: string;
        };
        name: {
            name: string;
        };
        vector: {
            name: string;
        };
    };
    cmd: {
        list: {
            size: number;
        };
    };
    milvus: any;
};
export type MilvusStoreOptions = Partial<Options>;
declare function MilvusStore(this: any, options: Options): {
    name: string;
    tag: any;
    exportmap: {
        native: () => {
            client: any;
        };
    };
};
export default MilvusStore;
