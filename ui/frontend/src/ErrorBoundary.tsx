import { Component } from "react";
import type { ErrorInfo, ReactNode } from "react";

// A render error anywhere under the root unmounts the whole tree in React 18,
// which on this app's dark surfaces looks like the window going black — no
// message, nothing to report but "it stopped working". This turns that into
// the stack trace, in place, with the rest of the app still standing.
type Props = { label: string; children: ReactNode };
type State = { error: Error | null; stack: string };

export default class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null, stack: "" };

  static getDerivedStateFromError(error: Error): Partial<State> {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // The component stack says which panel blew up, which the error's own
    // stack does not once the bundle is minified.
    this.setState({ stack: info.componentStack ?? "" });
    console.error(`${this.props.label} crashed:`, error, info.componentStack);
  }

  render() {
    const { error, stack } = this.state;
    if (!error) return this.props.children;
    return (
      <div className="render-error">
        <div className="render-error-head">
          ⚠ {this.props.label} failed to render — the rest of the app is still
          usable.
        </div>
        <pre className="render-error-body">
          {String(error.stack || error)}
          {stack}
        </pre>
      </div>
    );
  }
}
