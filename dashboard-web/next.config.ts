import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "export",
  basePath: "/app",
  images: { unoptimized: true },
};

if (process.env.NODE_ENV === "development") {
  nextConfig.rewrites = async () => [
    { source: "/api/:path*", destination: "http://127.0.0.1:8000/api/:path*" },
    { source: "/import-bill-detail", destination: "http://127.0.0.1:8000/import-bill-detail" },
  ];
}

export default nextConfig;
