const PRIVATE_API_BINDING = "database"

function jsonError(status, payload) {
  return Response.json(payload, {
    status,
    headers: {
      "Cache-Control": "no-store",
    },
  })
}

export async function forwardToPrivateApi(context) {
  const privateApi = context.env?.[PRIVATE_API_BINDING]

  if (!privateApi || typeof privateApi.fetch !== "function") {
    return jsonError(503, { status: "private_api_binding_unavailable" })
  }

  try {
    return await privateApi.fetch(context.request)
  } catch (error) {
    console.error("Private API service binding request failed", error)
    return jsonError(502, { status: "private_api_unavailable" })
  }
}
