export namespace main {
	
	export class ChatMessage {
	    role: string;
	    content: string;
	
	    static createFrom(source: any = {}) {
	        return new ChatMessage(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.role = source["role"];
	        this.content = source["content"];
	    }
	}
	export class ModelOption {
	    id: string;
	    label: string;
	
	    static createFrom(source: any = {}) {
	        return new ModelOption(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.label = source["label"];
	    }
	}
	export class QueryResult {
	    columns: string[];
	    rows: string[][];
	    truncated: boolean;
	    elapsedMs: number;
	
	    static createFrom(source: any = {}) {
	        return new QueryResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.columns = source["columns"];
	        this.rows = source["rows"];
	        this.truncated = source["truncated"];
	        this.elapsedMs = source["elapsedMs"];
	    }
	}
	export class RunResult {
	    portfolioName: string;
	    strategy: string;
	    sharpeRatio: number;
	    sortinoRatio: number;
	    maxDrawdown: number;
	    annualReturn: number;
	    standardDev: number;
	    avgCorrelation: number;
	    cointegratedPairs: number;
	    initialCapital: number;
	    finalValue: number;
	    equityCurve: number[];
	    dates: string[];
	
	    static createFrom(source: any = {}) {
	        return new RunResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.portfolioName = source["portfolioName"];
	        this.strategy = source["strategy"];
	        this.sharpeRatio = source["sharpeRatio"];
	        this.sortinoRatio = source["sortinoRatio"];
	        this.maxDrawdown = source["maxDrawdown"];
	        this.annualReturn = source["annualReturn"];
	        this.standardDev = source["standardDev"];
	        this.avgCorrelation = source["avgCorrelation"];
	        this.cointegratedPairs = source["cointegratedPairs"];
	        this.initialCapital = source["initialCapital"];
	        this.finalValue = source["finalValue"];
	        this.equityCurve = source["equityCurve"];
	        this.dates = source["dates"];
	    }
	}

}

