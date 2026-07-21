const ALLOWED_ORIGINS = new Set([
  "https://genshin-text-search.pages.dev",
]);

const ALLOWED_METHODS = "GET, HEAD, POST, OPTIONS";
const ALLOWED_HEADERS = "Content-Type";
const FORWARDED_REQUEST_HEADERS = [
  "Accept",
  "Accept-Language",
  "Content-Type",
  "If-Modified-Since",
  "If-None-Match",
  "Origin",
  "Range",
  "User-Agent",
];

function corsHeaders(request) {
  const origin = request.headers.get("Origin");
  const headers = new Headers({ Vary: "Origin" });

  if (origin && ALLOWED_ORIGINS.has(origin)) {
    headers.set("Access-Control-Allow-Origin", origin);
    headers.set("Access-Control-Allow-Methods", ALLOWED_METHODS);
    headers.set("Access-Control-Allow-Headers", ALLOWED_HEADERS);
    headers.set("Access-Control-Max-Age", "86400");
  }

  return headers;
}

function jsonResponse(request, status, payload) {
  const headers = corsHeaders(request);
  headers.set("Content-Type", "application/json; charset=utf-8");
  headers.set("Cache-Control", "no-store");
  return new Response(JSON.stringify(payload), { status, headers });
}

function withCors(request, response) {
  const headers = new Headers(response.headers);
  for (const [name, value] of corsHeaders(request)) {
    headers.set(name, value);
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function isAllowedPath(pathname) {
  return pathname === "/healthz" || pathname.startsWith("/api/");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin");

    if (!isAllowedPath(url.pathname)) {
      return jsonResponse(request, 404, { status: "not_found" });
    }

    if (origin && !ALLOWED_ORIGINS.has(origin)) {
      return jsonResponse(request, 403, { status: "origin_not_allowed" });
    }

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders(request) });
    }

    if (!new Set(["GET", "HEAD", "POST"]).has(request.method)) {
      return jsonResponse(request, 405, { status: "method_not_allowed" });
    }

    const targetUrl = new URL(
      `http://127.0.0.1:5055${url.pathname}${url.search}`,
    );
    const headers = new Headers();
    const clientIp = request.headers.get("CF-Connecting-IP");

    for (const name of FORWARDED_REQUEST_HEADERS) {
      const value = request.headers.get(name);
      if (value) {
        headers.set(name, value);
      }
    }
    if (clientIp) {
      headers.set("X-Forwarded-For", clientIp);
    }
    headers.set("X-Forwarded-Proto", "https");
    headers.set("X-Forwarded-Host", url.host);

    const proxyRequest = new Request(targetUrl, {
      method: request.method,
      headers,
      body: request.method === "GET" || request.method === "HEAD"
        ? undefined
        : request.body,
      redirect: "manual",
    });

    try {
      const response = await env.PRIVATE_API.fetch(proxyRequest);
      return withCors(request, response);
    } catch (error) {
      console.error("VPC proxy request failed", error);
      return jsonResponse(request, 502, { status: "upstream_unavailable" });
    }
  },
};
