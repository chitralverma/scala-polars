pub mod csv;
pub mod ipc;
pub mod json_lines;
pub mod parquet;

use jni::Env;
use jni::objects::{JObjectArray, JString};
use polars::io::cloud::CloudOptions;
use polars::prelude::*;

use crate::internal_jni::io::parse_cloud_options;
use crate::internal_jni::utils::j_string_array_to_vec;

/// Builds the [`ScanSources`] from the JVM path array and derives cloud options from the remaining
/// `options`. `parse_cloud_options` consumes the map, so callers must strip format-specific keys first.
pub(crate) fn build_scan_sources(
    env: &mut Env,
    paths: &JObjectArray<JString>,
    options: PlHashMap<String, String>,
) -> anyhow::Result<(ScanSources, Option<CloudOptions>)> {
    let paths_vec: Vec<PlRefPath> =
        j_string_array_to_vec(env, paths, "Failed to get provided path")?
            .into_iter()
            .map(PlRefPath::new)
            .collect();

    let sources = ScanSources::Paths(paths_vec.into());
    let cloud_scheme = sources
        .first_path()
        .cloned()
        .as_ref()
        .and_then(|x| x.scheme());

    let cloud_options = parse_cloud_options(cloud_scheme, options)?;

    Ok((sources, cloud_options))
}
