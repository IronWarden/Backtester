export namespace backtest {
	
	export class Drawdown {
	    // Go type: time
	    start: any;
	    // Go type: time
	    trough: any;
	    // Go type: time
	    recovered: any;
	    ongoing: boolean;
	    depthPct: number;
	    durationDays: number;
	    recoveryDays: number;
	
	    static createFrom(source: any = {}) {
	        return new Drawdown(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.start = this.convertValues(source["start"], null);
	        this.trough = this.convertValues(source["trough"], null);
	        this.recovered = this.convertValues(source["recovered"], null);
	        this.ongoing = source["ongoing"];
	        this.depthPct = source["depthPct"];
	        this.durationDays = source["durationDays"];
	        this.recoveryDays = source["recoveryDays"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class PeriodReturn {
	    period: string;
	    returnPct: number;
	    partial: boolean;
	    // Go type: time
	    start: any;
	    // Go type: time
	    end: any;
	
	    static createFrom(source: any = {}) {
	        return new PeriodReturn(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.period = source["period"];
	        this.returnPct = source["returnPct"];
	        this.partial = source["partial"];
	        this.start = this.convertValues(source["start"], null);
	        this.end = this.convertValues(source["end"], null);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class RollingPoint {
	    // Go type: time
	    date: any;
	    value: number;
	
	    static createFrom(source: any = {}) {
	        return new RollingPoint(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.date = this.convertValues(source["date"], null);
	        this.value = source["value"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}

}

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
	    turnover: number;
	    alpha: number;
	    beta: number;
	    trackingError: number;
	    informationRatio: number;
	    upCapture: number;
	    downCapture: number;
	    initialCapital: number;
	    finalValue: number;
	    equityCurve: number[];
	    dates: string[];
	    drawdowns: backtest.Drawdown[];
	    rollingSharpe: backtest.RollingPoint[];
	    yearlyReturns: backtest.PeriodReturn[];
	    monthlyReturns: backtest.PeriodReturn[];
	
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
	        this.turnover = source["turnover"];
	        this.alpha = source["alpha"];
	        this.beta = source["beta"];
	        this.trackingError = source["trackingError"];
	        this.informationRatio = source["informationRatio"];
	        this.upCapture = source["upCapture"];
	        this.downCapture = source["downCapture"];
	        this.initialCapital = source["initialCapital"];
	        this.finalValue = source["finalValue"];
	        this.equityCurve = source["equityCurve"];
	        this.dates = source["dates"];
	        this.drawdowns = this.convertValues(source["drawdowns"], backtest.Drawdown);
	        this.rollingSharpe = this.convertValues(source["rollingSharpe"], backtest.RollingPoint);
	        this.yearlyReturns = this.convertValues(source["yearlyReturns"], backtest.PeriodReturn);
	        this.monthlyReturns = this.convertValues(source["monthlyReturns"], backtest.PeriodReturn);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}

}

