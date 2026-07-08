use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tracing::error;

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    pub(crate) message: String,
    details: Option<serde_json::Value>,
}

impl ApiError {
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            details: None,
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
            details: None,
        }
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
            details: None,
        }
    }

    pub(crate) fn conflict_with_details(
        message: impl Into<String>,
        details: serde_json::Value,
    ) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
            details: Some(details),
        }
    }

    pub(crate) fn schema_version_conflict(
        client_schema_version: u32,
        server_schema_version: u32,
    ) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: format!(
                "client schema version {client_schema_version} does not match server schema version {server_schema_version}"
            ),
            details: Some(serde_json::json!({
                "schemaVersionMismatch": true,
                "clientSchemaVersion": client_schema_version,
                "serverSchemaVersion": server_schema_version,
            })),
        }
    }

    pub(crate) fn owner_conflict(shard: usize, owner: String, owner_url: String) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: format!("shard {shard} is owned by {owner}; retry against {owner_url}"),
            details: Some(serde_json::json!({
                "shard": shard,
                "owner": owner,
                "ownerUrl": owner_url,
            })),
        }
    }

    pub(crate) fn locked(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::LOCKED,
            message: message.into(),
            details: None,
        }
    }

    pub(crate) fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
            details: None,
        }
    }

    pub(crate) fn payload_too_large(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: message.into(),
            details: None,
        }
    }

    pub(crate) fn unavailable_with_details(
        message: impl Into<String>,
        details: serde_json::Value,
    ) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
            details: Some(details),
        }
    }

    pub(crate) fn range_not_satisfiable(total_size: u64, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::RANGE_NOT_SATISFIABLE,
            message: message.into(),
            details: Some(serde_json::json!({
                "contentRange": format!("bytes */{total_size}"),
                "byteSize": total_size,
            })),
        }
    }

    pub(crate) fn internal(error: anyhow::Error) -> Self {
        error!(?error, "request failed");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
            details: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut payload = serde_json::Map::new();
        payload.insert("error".to_string(), serde_json::Value::String(self.message));
        if let Some(serde_json::Value::Object(details)) = self.details {
            payload.extend(details);
        }
        (self.status, Json(serde_json::Value::Object(payload))).into_response()
    }
}
