import { forwardRef, type ButtonHTMLAttributes } from "react";
import { cn } from "@/lib/utils";

export type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "secondary" | "ghost";
};

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = "secondary", ...props }, ref) => {
    const base =
      "inline-flex items-center justify-center gap-2 rounded-lg px-4 py-2.5 text-sm font-semibold transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-slate-900 disabled:pointer-events-none disabled:opacity-50";
    const styles = {
      primary:
        "bg-slate-900 text-white shadow-sm hover:bg-slate-800 active:bg-slate-950",
      secondary:
        "border border-slate-200 bg-white text-slate-900 shadow-sm hover:bg-slate-50",
      ghost:
        "border border-dashed border-slate-300 bg-transparent text-slate-600 hover:border-slate-400 hover:text-slate-900",
    }[variant];
    return <button ref={ref} type="button" className={cn(base, styles, className)} {...props} />;
  },
);
Button.displayName = "Button";
