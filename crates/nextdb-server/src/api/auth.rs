use axum::{
    extract::{Request, State},
    http::{HeaderMap, Method, Uri, header},
    middleware::Next,
    response::Response,
};

use crate::{AppState, api::error::ApiError};

pub(crate) async fn auth_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let path = request.uri().path();
    let method = request.method();
    if requires_admin_auth(method, path)
        && !token_matches_request(
            request.headers(),
            request.uri(),
            state.admin_token.as_deref(),
            &["x-nextdb-admin-token"],
        )
    {
        return Err(ApiError::unauthorized("admin token is required"));
    }
    if requires_client_auth(method, path) {
        let client_auth_configured =
            state.client_token.is_some() || !state.client_user_tokens.is_empty();
        let client_ok = if client_auth_configured {
            state.client_token.is_some()
                && token_matches_request(
                    request.headers(),
                    request.uri(),
                    state.client_token.as_deref(),
                    &["x-nextdb-client-token"],
                )
                || token_matches_user_token_for_path(request.headers(), request.uri(), &state)
        } else {
            true
        };
        let admin_ok = state.admin_token.is_some()
            && token_matches_request(
                request.headers(),
                request.uri(),
                state.admin_token.as_deref(),
                &["x-nextdb-admin-token"],
            );
        if !client_ok && !admin_ok {
            return Err(ApiError::unauthorized("client token is required"));
        }
        ensure_request_schema_version(&state, request.headers(), request.uri())?;
    }
    Ok(next.run(request).await)
}

fn requires_admin_auth(method: &Method, path: &str) -> bool {
    if !path.starts_with("/v1/admin/") {
        return false;
    }
    if path == "/v1/admin/wal/replicate"
        || path == "/v1/admin/objects/replicate"
        || path == "/v1/admin/schema/preflight"
        || path == "/v1/admin/schema/proposals/prepare"
        || path == "/v1/admin/schema/proposals/commit"
        || path == "/v1/admin/schema/proposals/abort"
        || path == "/v1/admin/cluster/topology/proposals/prepare"
        || path == "/v1/admin/cluster/topology/proposals/commit"
        || path == "/v1/admin/cluster/topology/proposals/abort"
    {
        return false;
    }
    matches!(
        *method,
        Method::GET | Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

fn requires_client_auth(method: &Method, path: &str) -> bool {
    if path == "/v1/connect" || path == "/v1/connect/jsonl" {
        return true;
    }
    if path.starts_with("/v1/records/") {
        return matches!(*method, Method::POST | Method::DELETE);
    }
    if path.starts_with("/v1/objects/") {
        return matches!(*method, Method::DELETE);
    }
    if *method != Method::POST {
        return false;
    }
    path == "/v1/mutate"
        || path == "/v1/objects"
        || path.starts_with("/v1/realtime/channels/")
        || path == "/v1/behaviors/invoke"
}

pub(crate) fn token_matches_request(
    headers: &HeaderMap,
    uri: &Uri,
    expected: Option<&str>,
    header_names: &[&str],
) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    bearer_token(headers)
        .or_else(|| first_header_token(headers, header_names))
        .or_else(|| query_token(uri))
        .is_some_and(|token| token == expected)
}

fn token_matches_user_token(headers: &HeaderMap, uri: &Uri, state: &AppState) -> bool {
    request_token(headers, uri, &["x-nextdb-client-token"]).is_some_and(|token| {
        state
            .client_user_tokens
            .values()
            .any(|expected| expected == token)
    })
}

fn token_matches_user_token_for_path(headers: &HeaderMap, uri: &Uri, state: &AppState) -> bool {
    let path = uri.path();
    if path == "/v1/connect" || path == "/v1/connect/jsonl" {
        return query_param_value(uri, "userId").is_some()
            && token_matches_user_token(headers, uri, state);
    }
    if path != "/v1/mutate"
        && path != "/v1/behaviors/invoke"
        && !path.starts_with("/v1/realtime/channels/")
    {
        return false;
    }
    token_matches_user_token(headers, uri, state)
}

pub(crate) fn ensure_global_client_token_authorized(
    state: &AppState,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<(), ApiError> {
    if state.client_token.is_none() && state.admin_token.is_none() {
        if !state.client_user_tokens.is_empty() {
            return Err(ApiError::unauthorized("client token is required"));
        }
        return Ok(());
    }
    if state.client_token.is_some()
        && token_matches_request(
            headers,
            uri,
            state.client_token.as_deref(),
            &["x-nextdb-client-token"],
        )
    {
        return Ok(());
    }
    if state.admin_token.is_some()
        && token_matches_request(
            headers,
            uri,
            state.admin_token.as_deref(),
            &["x-nextdb-admin-token"],
        )
    {
        return Ok(());
    }
    Err(ApiError::unauthorized("client token is required"))
}

pub(crate) fn ensure_user_token_authorized(
    state: &AppState,
    headers: &HeaderMap,
    uri: &Uri,
    user_id: &str,
) -> Result<(), ApiError> {
    if state.client_user_tokens.is_empty() {
        return Ok(());
    }
    if state.admin_token.is_some()
        && token_matches_request(
            headers,
            uri,
            state.admin_token.as_deref(),
            &["x-nextdb-admin-token"],
        )
    {
        return Ok(());
    }
    let Some(expected) = state.client_user_tokens.get(user_id) else {
        return Err(ApiError::unauthorized("user token is required"));
    };
    if request_token(headers, uri, &["x-nextdb-client-token"])
        .is_some_and(|token| token == expected)
    {
        return Ok(());
    }
    Err(ApiError::unauthorized(
        "client token is not authorized for userId",
    ))
}

fn request_token<'a>(
    headers: &'a HeaderMap,
    uri: &'a Uri,
    header_names: &[&str],
) -> Option<&'a str> {
    bearer_token(headers)
        .or_else(|| first_header_token(headers, header_names))
        .or_else(|| query_token(uri))
}

fn ensure_request_schema_version(
    state: &AppState,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<(), ApiError> {
    let Some(client_schema_version) = request_schema_version(headers, uri)? else {
        return Ok(());
    };
    let server_schema_version = state.schema.version();
    if client_schema_version == server_schema_version {
        return Ok(());
    }
    Err(ApiError::schema_version_conflict(
        client_schema_version,
        server_schema_version,
    ))
}

fn request_schema_version(headers: &HeaderMap, uri: &Uri) -> Result<Option<u32>, ApiError> {
    if let Some(value) = headers
        .get("x-nextdb-schema-version")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return value.parse::<u32>().map(Some).map_err(|_| {
            ApiError::bad_request("x-nextdb-schema-version must be an unsigned integer")
        });
    }
    let Some(query) = uri.query() else {
        return Ok(None);
    };
    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        if key == "schemaVersion" {
            return value
                .trim()
                .parse::<u32>()
                .map(Some)
                .map_err(|_| ApiError::bad_request("schemaVersion must be an unsigned integer"));
        }
    }
    Ok(None)
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?.trim();
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn first_header_token<'a>(headers: &'a HeaderMap, names: &[&str]) -> Option<&'a str> {
    for name in names {
        if let Some(value) = headers
            .get(*name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Some(value);
        }
    }
    None
}

fn query_token(uri: &Uri) -> Option<&str> {
    uri.query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        if key == "authToken" || key == "adminToken" {
            Some(value)
        } else {
            None
        }
    })
}

fn query_param_value<'a>(uri: &'a Uri, expected_key: &str) -> Option<&'a str> {
    uri.query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        if key == expected_key && !value.trim().is_empty() {
            Some(value)
        } else {
            None
        }
    })
}
