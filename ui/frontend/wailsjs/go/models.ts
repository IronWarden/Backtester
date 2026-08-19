export namespace backtest {
	
	export class BaselineStats {
	    name: string;
	    finalValue: number;
	    annualReturn: number;
	    sharpeRatio: number;
	    maxDrawdown: number;
	    standardDev: number;
	    excessFinalValue: number;
	    excessAnnualReturn: number;
	    beat: boolean;
	    computed: boolean;
	
	    static createFrom(source: any = {}) {
	        return new BaselineStats(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.finalValue = source["finalValue"];
	        this.annualReturn = source["annualReturn"];
	        this.sharpeRatio = source["sharpeRatio"];
	        this.maxDrawdown = source["maxDrawdown"];
	        this.standardDev = source["standardDev"];
	        this.excessFinalValue = source["excessFinalValue"];
	        this.excessAnnualReturn = source["excessAnnualReturn"];
	        this.beat = source["beat"];
	        this.computed = source["computed"];
	    }
	}
	export class BenchmarkStats {
	    ticker: string;
	    annualReturn: number;
	    sharpeRatio: number;
	    maxDrawdown: number;
	    standardDev: number;
	
	    static createFrom(source: any = {}) {
	        return new BenchmarkStats(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.ticker = source["ticker"];
	        this.annualReturn = source["annualReturn"];
	        this.sharpeRatio = source["sharpeRatio"];
	        this.maxDrawdown = source["maxDrawdown"];
	        this.standardDev = source["standardDev"];
	    }
	}
	export class Diagnostic {
	    level: string;
	    portfolio: string;
	    message: string;
	
	    static createFrom(source: any = {}) {
	        return new Diagnostic(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.level = source["level"];
	        this.portfolio = source["portfolio"];
	        this.message = source["message"];
	    }
	}
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
	export class SegmentStats {
	    label: string;
	    start: string;
	    end: string;
	    days: number;
	    totalReturn: number;
	    annualReturn: number;
	    sharpeRatio: number;
	    sortinoRatio: number;
	    maxDrawdown: number;
	    standardDev: number;
	
	    static createFrom(source: any = {}) {
	        return new SegmentStats(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.label = source["label"];
	        this.start = source["start"];
	        this.end = source["end"];
	        this.days = source["days"];
	        this.totalReturn = source["totalReturn"];
	        this.annualReturn = source["annualReturn"];
	        this.sharpeRatio = source["sharpeRatio"];
	        this.sortinoRatio = source["sortinoRatio"];
	        this.maxDrawdown = source["maxDrawdown"];
	        this.standardDev = source["standardDev"];
	    }
	}
	export class Significance {
	    draws: number;
	    seed: number;
	    sharpe: number;
	    pValue: number;
	    percentile: number;
	    nullMeanSharpe: number;
	    nullP95Sharpe: number;
	    bootstrapLow: number;
	    bootstrapHigh: number;
	    computed: boolean;
	
	    static createFrom(source: any = {}) {
	        return new Significance(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.draws = source["draws"];
	        this.seed = source["seed"];
	        this.sharpe = source["sharpe"];
	        this.pValue = source["pValue"];
	        this.percentile = source["percentile"];
	        this.nullMeanSharpe = source["nullMeanSharpe"];
	        this.nullP95Sharpe = source["nullP95Sharpe"];
	        this.bootstrapLow = source["bootstrapLow"];
	        this.bootstrapHigh = source["bootstrapHigh"];
	        this.computed = source["computed"];
	    }
	}
	export class WalkForwardWindow {
	    trainStart: string;
	    trainEnd: string;
	    testStart: string;
	    testEnd: string;
	    selected: string;
	    trainScore: number;
	    testReturn: number;
	    candidates: number;
	    capital: number;
	
	    static createFrom(source: any = {}) {
	        return new WalkForwardWindow(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.trainStart = source["trainStart"];
	        this.trainEnd = source["trainEnd"];
	        this.testStart = source["testStart"];
	        this.testEnd = source["testEnd"];
	        this.selected = source["selected"];
	        this.trainScore = source["trainScore"];
	        this.testReturn = source["testReturn"];
	        this.candidates = source["candidates"];
	        this.capital = source["capital"];
	    }
	}
	export class WalkForwardResult {
	    objective: string;
	    windows: WalkForwardWindow[];
	
	    static createFrom(source: any = {}) {
	        return new WalkForwardResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.objective = source["objective"];
	        this.windows = this.convertValues(source["windows"], WalkForwardWindow);
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
	    avgExposure: number;
	    calmarRatio: number;
	    longestUnderwaterDays: number;
	    timeInMarket: number;
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
	    benchmarkCurve: number[];
	    benchmarkStats: backtest.BenchmarkStats;
	    splits: backtest.SegmentStats[];
	    walkForward?: backtest.WalkForwardResult;
	    trials: number;
	    expectedMaxSharpe: number;
	    deflatedSharpe: number;
	    baseline: backtest.BaselineStats;
	    significance: backtest.Significance;
	
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
	        this.avgExposure = source["avgExposure"];
	        this.calmarRatio = source["calmarRatio"];
	        this.longestUnderwaterDays = source["longestUnderwaterDays"];
	        this.timeInMarket = source["timeInMarket"];
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
	        this.benchmarkCurve = source["benchmarkCurve"];
	        this.benchmarkStats = this.convertValues(source["benchmarkStats"], backtest.BenchmarkStats);
	        this.splits = this.convertValues(source["splits"], backtest.SegmentStats);
	        this.walkForward = this.convertValues(source["walkForward"], backtest.WalkForwardResult);
	        this.trials = source["trials"];
	        this.expectedMaxSharpe = source["expectedMaxSharpe"];
	        this.deflatedSharpe = source["deflatedSharpe"];
	        this.baseline = this.convertValues(source["baseline"], backtest.BaselineStats);
	        this.significance = this.convertValues(source["significance"], backtest.Significance);
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
	export class RunOutput {
	    results: RunResult[];
	    diagnostics: backtest.Diagnostic[];
	
	    static createFrom(source: any = {}) {
	        return new RunOutput(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.results = this.convertValues(source["results"], RunResult);
	        this.diagnostics = this.convertValues(source["diagnostics"], backtest.Diagnostic);
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

