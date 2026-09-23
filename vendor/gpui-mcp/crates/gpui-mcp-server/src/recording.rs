use std::ffi::OsStr;
use std::fs;
use std::io::{self, Seek, SeekFrom, Write as _};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use image::RgbaImage;
use mp4::{AvcConfig, FourCC, Mp4Config, Mp4Sample, Mp4Writer, TrackConfig};
use openh264::{
    OpenH264API,
    encoder::{
        Complexity, Encoder, EncoderConfig, FrameRate, FrameType, RateControlMode, UsageType,
    },
    formats::{RgbSliceU8, YUVBuffer},
};
use tempfile::{NamedTempFile, TempPath};

pub(crate) const MAX_RECORDING_FRAMES: usize = 30 * 120;
const MAX_ARTIFACT_NAME_BYTES: usize = 128;
const MAX_RECORDING_FRAME_DIMENSION: u32 = 4_096;
const MAX_RECORDING_FRAME_PIXELS: u64 = 16 * 1024 * 1024;
const MAX_RECORDING_OUTPUT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrameTiming {
    frames_per_second: u8,
}

impl FrameTiming {
    pub(crate) fn frames_per_second(value: u8) -> Result<Self, String> {
        if (1..=30).contains(&value) {
            Ok(Self {
                frames_per_second: value,
            })
        } else {
            Err("frames_per_second must be between 1 and 30".to_owned())
        }
    }

    pub(crate) const fn configured_frames_per_second(self) -> u8 {
        self.frames_per_second
    }

    const fn timescale(self) -> u32 {
        self.frames_per_second as u32
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ArtifactStore {
    directory: Arc<PathBuf>,
}

impl ArtifactStore {
    pub(crate) fn open(directory: impl AsRef<Path>) -> Result<Self, String> {
        let directory = directory.as_ref();
        match fs::symlink_metadata(directory) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!(
                    "recording artifact directory {} must not be a symlink",
                    directory.display()
                ));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(format!(
                    "recording artifact directory {} is not a directory",
                    directory.display()
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(directory).map_err(|error| {
                    format!(
                        "could not create recording artifact directory {}: {error}",
                        directory.display()
                    )
                })?;
            }
            Err(error) => {
                return Err(format!(
                    "could not inspect recording artifact directory {}: {error}",
                    directory.display()
                ));
            }
        }
        let metadata = fs::symlink_metadata(directory).map_err(|error| {
            format!(
                "could not inspect recording artifact directory {}: {error}",
                directory.display()
            )
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(format!(
                "recording artifact directory {} must be a real directory",
                directory.display()
            ));
        }
        #[cfg(unix)]
        secure_directory_permissions(directory)?;
        let directory = fs::canonicalize(directory).map_err(|error| {
            format!(
                "could not resolve recording artifact directory {}: {error}",
                directory.display()
            )
        })?;
        Ok(Self {
            directory: Arc::new(directory),
        })
    }

    pub(crate) fn directory(&self) -> &Path {
        self.directory.as_path()
    }

    fn prepare(&self, artifact_name: &str, overwrite: bool) -> Result<PreparedArtifact, String> {
        validate_artifact_name(artifact_name)?;
        let final_path = self.directory.join(artifact_name);
        validate_existing_artifact(&final_path, overwrite)?;
        let temporary = NamedTempFile::new_in(self.directory()).map_err(|error| {
            format!(
                "could not create a temporary recording artifact in {}: {error}",
                self.directory().display()
            )
        })?;
        let (file, temporary_path) = temporary.into_parts();
        Ok(PreparedArtifact {
            file,
            temporary_path,
            final_path,
            overwrite,
        })
    }
}

struct PreparedArtifact {
    file: fs::File,
    temporary_path: TempPath,
    final_path: PathBuf,
    overwrite: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecordingArtifact {
    pub(crate) path: PathBuf,
    pub(crate) frame_count: usize,
    pub(crate) timeline_frames: u64,
    pub(crate) timing: FrameTiming,
    pub(crate) duration_ms: u64,
    pub(crate) bytes: u64,
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) dropped_frames: u32,
}

/// Incremental H.264/MP4 writer used by the live capture worker.
///
/// Frames are converted and written immediately. No PNGs, base64 payloads, or decoded
/// frame history are retained while recording.
pub(crate) struct LiveRecorder {
    encoder: Encoder,
    rgb: Vec<u8>,
    yuv: YUVBuffer,
    writer: Option<Mp4Writer<BoundedSeekWriter<fs::File>>>,
    temporary_path: Option<TempPath>,
    final_path: PathBuf,
    overwrite: bool,
    source_dimensions: (u32, u32),
    video_dimensions: (u32, u32),
    timing: FrameTiming,
    frame_count: usize,
    timeline_ticks: u64,
    dropped_frames: u32,
}

impl LiveRecorder {
    pub(crate) fn start(
        store: &ArtifactStore,
        artifact_name: &str,
        overwrite: bool,
        timing: FrameTiming,
        first_frame: RgbaImage,
    ) -> Result<Self, String> {
        let source_dimensions = validate_rgba_frame(&first_frame, None)?;
        let video_dimensions = padded_video_dimensions(source_dimensions)?;
        let prepared = store.prepare(artifact_name, overwrite)?;
        let config = EncoderConfig::new()
            .usage_type(UsageType::ScreenContentRealTime)
            .complexity(Complexity::Low)
            .max_frame_rate(FrameRate::from_hz(f32::from(
                timing.configured_frames_per_second(),
            )))
            .rate_control_mode(RateControlMode::Off)
            .adaptive_quantization(false)
            .background_detection(false)
            .skip_frames(false);
        let mut encoder = Encoder::with_api_config(OpenH264API::from_source(), config)
            .map_err(|error| format!("could not initialize H.264 video encoder: {error}"))?;
        let first_frame = pad_frame(first_frame, video_dimensions);
        let (mut rgb, mut yuv) = conversion_buffers(video_dimensions)?;
        let first_sample = encode_h264_frame(
            &mut encoder,
            &first_frame,
            video_dimensions,
            &mut rgb,
            &mut yuv,
        )?;
        let avc = avc_config(&first_sample, video_dimensions)?;
        let bounded = BoundedSeekWriter::new(prepared.file, MAX_RECORDING_OUTPUT_BYTES)
            .map_err(|error| format!("could not initialize MP4 output: {error}"))?;
        let config = Mp4Config {
            major_brand: FourCC::from(*b"isom"),
            minor_version: 512,
            compatible_brands: vec![
                FourCC::from(*b"isom"),
                FourCC::from(*b"iso2"),
                FourCC::from(*b"avc1"),
                FourCC::from(*b"mp41"),
            ],
            timescale: timing.timescale(),
        };
        let mut writer = Mp4Writer::write_start(bounded, &config)
            .map_err(|error| format!("could not initialize MP4 video container: {error}"))?;
        let mut track = TrackConfig::from(avc);
        track.timescale = timing.timescale();
        writer
            .add_track(&track)
            .map_err(|error| format!("could not add H.264 video track: {error}"))?;
        write_h264_sample(&mut writer, first_sample, 0, 1)?;

        Ok(Self {
            encoder,
            rgb,
            yuv,
            writer: Some(writer),
            temporary_path: Some(prepared.temporary_path),
            final_path: prepared.final_path,
            overwrite: prepared.overwrite,
            source_dimensions,
            video_dimensions,
            timing,
            frame_count: 1,
            timeline_ticks: 1,
            dropped_frames: 0,
        })
    }

    #[cfg(test)]
    fn push(&mut self, frame: RgbaImage) -> Result<(), String> {
        self.push_for(frame, 1)
    }

    pub(crate) fn push_for(&mut self, frame: RgbaImage, duration_ticks: u32) -> Result<(), String> {
        if duration_ticks == 0 {
            return Err("recording sample duration must be positive".to_owned());
        }
        if self.is_full() {
            return Err(format!(
                "recording reached the {} second safety limit",
                MAX_RECORDING_FRAMES / 30
            ));
        }
        validate_rgba_frame(&frame, Some(self.source_dimensions))?;
        let frame = pad_frame(frame, self.video_dimensions);
        let sample = encode_h264_frame(
            &mut self.encoder,
            &frame,
            self.video_dimensions,
            &mut self.rgb,
            &mut self.yuv,
        )?;
        let start_time = self.timeline_ticks;
        let maximum_ticks = u64::from(self.timing.frames_per_second) * 120;
        let duration_ticks =
            u32::try_from(u64::from(duration_ticks).min(maximum_ticks.saturating_sub(start_time)))
                .map_err(|_| "video sample duration overflowed".to_owned())?;
        if duration_ticks == 0 {
            return Err("recording reached the 120 second safety limit".to_owned());
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| "recording writer was already finalized".to_owned())?;
        write_h264_sample(writer, sample, start_time, duration_ticks)?;
        self.frame_count += 1;
        self.timeline_ticks = self
            .timeline_ticks
            .checked_add(u64::from(duration_ticks))
            .ok_or_else(|| "video timeline overflowed".to_owned())?;
        Ok(())
    }

    pub(crate) fn record_drop(&mut self) {
        self.dropped_frames = self.dropped_frames.saturating_add(1);
    }

    pub(crate) fn is_full(&self) -> bool {
        self.timeline_ticks >= u64::from(self.timing.frames_per_second) * 120
    }

    pub(crate) fn finish(mut self) -> Result<RecordingArtifact, String> {
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| "recording writer was already finalized".to_owned())?;
        writer
            .write_end()
            .map_err(|error| format!("could not finalize MP4 video container: {error}"))?;
        let mut output = writer.into_writer();
        output
            .flush()
            .map_err(|error| format!("could not flush MP4 video data: {error}"))?;
        output
            .inner
            .sync_all()
            .map_err(|error| format!("could not synchronize MP4 data: {error}"))?;
        let bytes = output.bytes_written();
        if bytes == 0 || bytes > MAX_RECORDING_OUTPUT_BYTES {
            return Err("temporary recording artifact has an invalid size".to_owned());
        }
        drop(output.inner);

        let temporary_path = self
            .temporary_path
            .take()
            .ok_or_else(|| "recording temporary path was already finalized".to_owned())?;
        if self.overwrite {
            temporary_path.persist(&self.final_path)
        } else {
            temporary_path.persist_noclobber(&self.final_path)
        }
        .map_err(|error| {
            format!(
                "could not atomically install recording artifact {}: {error}",
                self.final_path.display()
            )
        })?;

        #[cfg(unix)]
        if let Some(directory) = self.final_path.parent()
            && let Err(error) = sync_parent_directory(directory)
        {
            tracing::warn!(%error, "could not synchronize recording artifact directory");
        }

        Ok(RecordingArtifact {
            path: self.final_path,
            frame_count: self.frame_count,
            timeline_frames: self.timeline_ticks,
            timing: self.timing,
            duration_ms: self
                .timeline_ticks
                .checked_mul(1_000)
                .and_then(|duration| duration.checked_div(u64::from(self.timing.frames_per_second)))
                .ok_or_else(|| "video duration overflowed".to_owned())?,
            bytes,
            width: self.video_dimensions.0,
            height: self.video_dimensions.1,
            dropped_frames: self.dropped_frames,
        })
    }
}

pub(crate) fn validate_artifact_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > MAX_ARTIFACT_NAME_BYTES {
        return Err(format!(
            "artifact_name must contain 1 through {MAX_ARTIFACT_NAME_BYTES} ASCII bytes"
        ));
    }
    if Path::new(name).extension() != Some(OsStr::new("mp4")) {
        return Err("artifact_name must end with lowercase .mp4".to_owned());
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        || !name
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
    {
        return Err(
            "artifact_name may contain only ASCII letters, digits, '.', '_', and '-', and must start with a letter or digit"
                .to_owned(),
        );
    }
    let mut components = Path::new(name).components();
    if !matches!(components.next(), Some(Component::Normal(component)) if component == OsStr::new(name))
        || components.next().is_some()
    {
        return Err("artifact_name must be one filename without path separators".to_owned());
    }
    let stem = name.strip_suffix(".mp4").unwrap_or_default();
    if stem.is_empty() || is_windows_device_name(stem) {
        return Err("artifact_name is not a portable regular filename".to_owned());
    }
    Ok(())
}

fn validate_rgba_frame(
    frame: &RgbaImage,
    expected: Option<(u32, u32)>,
) -> Result<(u32, u32), String> {
    let dimensions = frame.dimensions();
    if dimensions.0 == 0
        || dimensions.1 == 0
        || dimensions.0 > MAX_RECORDING_FRAME_DIMENSION
        || dimensions.1 > MAX_RECORDING_FRAME_DIMENSION
        || u64::from(dimensions.0) * u64::from(dimensions.1) > MAX_RECORDING_FRAME_PIXELS
    {
        return Err("recording frame dimensions exceed the safety bound".to_owned());
    }
    if expected.is_some_and(|expected| expected != dimensions) {
        return Err(format!(
            "recording frame dimensions {}x{} changed during capture",
            dimensions.0, dimensions.1
        ));
    }
    Ok(dimensions)
}

fn padded_video_dimensions(dimensions: (u32, u32)) -> Result<(u32, u32), String> {
    let width = dimensions
        .0
        .checked_add(dimensions.0 % 2)
        .ok_or_else(|| "video width overflowed".to_owned())?
        .max(16);
    let height = dimensions
        .1
        .checked_add(dimensions.1 % 2)
        .ok_or_else(|| "video height overflowed".to_owned())?
        .max(16);
    if width > u32::from(u16::MAX) || height > u32::from(u16::MAX) {
        return Err("video dimensions exceed the MP4 H.264 limit".to_owned());
    }
    Ok((width, height))
}

fn pad_frame(frame: RgbaImage, dimensions: (u32, u32)) -> RgbaImage {
    if frame.dimensions() == dimensions {
        return frame;
    }
    let mut padded = RgbaImage::new(dimensions.0, dimensions.1);
    image::imageops::replace(&mut padded, &frame, 0, 0);
    padded
}

struct H264Sample {
    payload: Vec<u8>,
    nals: Vec<Vec<u8>>,
    sync: bool,
}

fn encode_h264_frame(
    encoder: &mut Encoder,
    rgba: &RgbaImage,
    dimensions: (u32, u32),
    rgb: &mut [u8],
    yuv: &mut YUVBuffer,
) -> Result<H264Sample, String> {
    let width = usize::try_from(dimensions.0)
        .map_err(|_| "video width does not fit the encoder".to_owned())?;
    let height = usize::try_from(dimensions.1)
        .map_err(|_| "video height does not fit the encoder".to_owned())?;
    rgba_to_rgb(rgba.as_raw(), rgb)?;
    yuv.read_rgb8(RgbSliceU8::new(rgb, (width, height)));
    let stream = encoder
        .encode(yuv)
        .map_err(|error| format!("could not encode H.264 video frame: {error}"))?;
    let sync = stream.frame_type() == FrameType::IDR;
    let nals = split_annex_b_nals(&stream.to_vec())?;
    let mut payload = Vec::new();
    for nal in &nals {
        let nal_type = nal.first().copied().unwrap_or_default() & 0x1f;
        if matches!(nal_type, 7..=9) {
            continue;
        }
        let length = u32::try_from(nal.len())
            .map_err(|_| "H.264 NAL unit exceeds the MP4 limit".to_owned())?;
        payload.extend_from_slice(&length.to_be_bytes());
        payload.extend_from_slice(nal);
    }
    if payload.is_empty() {
        return Err("H.264 encoder produced no displayable video frame".to_owned());
    }
    Ok(H264Sample {
        payload,
        nals,
        sync,
    })
}

fn conversion_buffers(dimensions: (u32, u32)) -> Result<(Vec<u8>, YUVBuffer), String> {
    let width = usize::try_from(dimensions.0)
        .map_err(|_| "video width does not fit the color converter".to_owned())?;
    let height = usize::try_from(dimensions.1)
        .map_err(|_| "video height does not fit the color converter".to_owned())?;
    let pixels = width
        .checked_mul(height)
        .ok_or_else(|| "video color buffer dimensions overflowed".to_owned())?;
    let rgb_len = pixels
        .checked_mul(3)
        .ok_or_else(|| "video RGB buffer size overflowed".to_owned())?;
    Ok((vec![0; rgb_len], YUVBuffer::new(width, height)))
}

fn rgba_to_rgb(rgba: &[u8], rgb: &mut [u8]) -> Result<(), String> {
    if !rgba.len().is_multiple_of(4)
        || !rgb.len().is_multiple_of(3)
        || rgba.len() / 4 != rgb.len() / 3
    {
        return Err("video RGBA and RGB conversion buffers do not match".to_owned());
    }
    for (source, destination) in rgba.chunks_exact(4).zip(rgb.chunks_exact_mut(3)) {
        destination.copy_from_slice(&source[..3]);
    }
    Ok(())
}

fn avc_config(sample: &H264Sample, dimensions: (u32, u32)) -> Result<AvcConfig, String> {
    let sps = sample
        .nals
        .iter()
        .find(|nal| nal.first().is_some_and(|byte| byte & 0x1f == 7))
        .cloned()
        .ok_or_else(|| "H.264 encoder did not provide a sequence parameter set".to_owned())?;
    let pps = sample
        .nals
        .iter()
        .find(|nal| nal.first().is_some_and(|byte| byte & 0x1f == 8))
        .cloned()
        .ok_or_else(|| "H.264 encoder did not provide a picture parameter set".to_owned())?;
    Ok(AvcConfig {
        width: u16::try_from(dimensions.0)
            .map_err(|_| "video width does not fit MP4 metadata".to_owned())?,
        height: u16::try_from(dimensions.1)
            .map_err(|_| "video height does not fit MP4 metadata".to_owned())?,
        seq_param_set: sps,
        pic_param_set: pps,
    })
}

fn write_h264_sample(
    writer: &mut Mp4Writer<BoundedSeekWriter<fs::File>>,
    sample: H264Sample,
    start_time: u64,
    duration: u32,
) -> Result<(), String> {
    writer
        .write_sample(
            1,
            &Mp4Sample {
                start_time,
                duration,
                rendering_offset: 0,
                is_sync: sample.sync,
                bytes: Bytes::from(sample.payload),
            },
        )
        .map_err(|error| format!("could not write H.264 MP4 frame: {error}"))
}

fn split_annex_b_nals(data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let starts = data
        .windows(4)
        .enumerate()
        .filter_map(|(index, bytes)| (bytes == [0, 0, 0, 1]).then_some(index))
        .collect::<Vec<_>>();
    if starts.is_empty() {
        return Err("H.264 encoder returned an invalid Annex B frame".to_owned());
    }
    let nals = starts
        .iter()
        .enumerate()
        .filter_map(|(index, start)| {
            let end = starts.get(index + 1).copied().unwrap_or(data.len());
            (end > start + 4).then(|| data[start + 4..end].to_vec())
        })
        .collect::<Vec<_>>();
    if nals.is_empty() {
        return Err("H.264 encoder returned no NAL units".to_owned());
    }
    Ok(nals)
}

fn validate_existing_artifact(path: &Path, overwrite: bool) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(format!(
            "refusing to replace symlink recording artifact {}",
            path.display()
        )),
        Ok(metadata) if !metadata.is_file() => Err(format!(
            "refusing to replace non-regular recording artifact {}",
            path.display()
        )),
        Ok(_) if !overwrite => Err(format!(
            "recording artifact {} already exists; pass overwrite=true to replace it",
            path.display()
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "could not inspect recording artifact {}: {error}",
            path.display()
        )),
    }
}

fn is_windows_device_name(stem: &str) -> bool {
    let base = stem.split('.').next().unwrap_or_default();
    base.eq_ignore_ascii_case("con")
        || base.eq_ignore_ascii_case("prn")
        || base.eq_ignore_ascii_case("aux")
        || base.eq_ignore_ascii_case("nul")
        || (base.len() == 4
            && (base[..3].eq_ignore_ascii_case("com") || base[..3].eq_ignore_ascii_case("lpt"))
            && matches!(base.as_bytes()[3], b'1'..=b'9'))
}

struct BoundedSeekWriter<W> {
    inner: W,
    limit: u64,
    position: u64,
    high_water_mark: u64,
}

impl<W: Seek> BoundedSeekWriter<W> {
    fn new(mut inner: W, limit: u64) -> io::Result<Self> {
        let position = inner.stream_position()?;
        Ok(Self {
            inner,
            limit,
            position,
            high_water_mark: position,
        })
    }

    const fn bytes_written(&self) -> u64 {
        self.high_water_mark
    }

    fn ensure_space(&self, length: usize) -> io::Result<()> {
        let length = u64::try_from(length)
            .map_err(|_| io::Error::other("recording output size overflowed"))?;
        if self.position.saturating_add(length) > self.limit {
            Err(io::Error::other(
                "recording output exceeds the 256 MiB safety bound",
            ))
        } else {
            Ok(())
        }
    }

    fn advance(&mut self, length: usize) -> io::Result<()> {
        let length = u64::try_from(length)
            .map_err(|_| io::Error::other("recording output size overflowed"))?;
        self.position = self
            .position
            .checked_add(length)
            .ok_or_else(|| io::Error::other("recording output size overflowed"))?;
        self.high_water_mark = self.high_water_mark.max(self.position);
        Ok(())
    }
}

impl<W: io::Write + Seek> io::Write for BoundedSeekWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.ensure_space(buffer.len())?;
        let written = self.inner.write(buffer)?;
        self.advance(written)?;
        Ok(written)
    }

    fn write_all(&mut self, buffer: &[u8]) -> io::Result<()> {
        self.ensure_space(buffer.len())?;
        self.inner.write_all(buffer)?;
        self.advance(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Seek> Seek for BoundedSeekWriter<W> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let position = self.inner.seek(position)?;
        if position > self.limit {
            return Err(io::Error::other(
                "recording output exceeds the 256 MiB safety bound",
            ));
        }
        self.position = position;
        self.high_water_mark = self.high_water_mark.max(position);
        Ok(position)
    }
}

#[cfg(unix)]
fn secure_directory_permissions(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|error| {
        format!(
            "could not secure recording artifact directory {}: {error}",
            path.display()
        )
    })
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), String> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            format!(
                "could not synchronize recording artifact directory {}: {error}",
                path.display()
            )
        })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use image::{Rgba, RgbaImage};
    use mp4::{MediaType, read_mp4};
    use tempfile::tempdir;

    use super::{ArtifactStore, FrameTiming, LiveRecorder, rgba_to_rgb, validate_artifact_name};

    #[test]
    fn artifact_names_are_portable_single_mp4_filenames() {
        assert!(validate_artifact_name("demo.mp4").is_ok());
        for invalid in [
            "../demo.mp4",
            "demo.MP4",
            ".demo.mp4",
            "CON.mp4",
            "demo/png",
        ] {
            assert!(
                validate_artifact_name(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn strips_alpha_into_the_reused_encoder_buffer() -> Result<(), String> {
        let rgba = [1, 2, 3, 4, 5, 6, 7, 8];
        let mut rgb = [0; 6];

        rgba_to_rgb(&rgba, &mut rgb)?;

        assert_eq!(rgb, [1, 2, 3, 5, 6, 7]);
        assert!(rgba_to_rgb(&rgba, &mut [0; 3]).is_err());
        Ok(())
    }

    #[test]
    fn live_recorder_writes_frames_incrementally() -> Result<(), String> {
        let directory = tempdir().map_err(|error| error.to_string())?;
        let store = ArtifactStore::open(directory.path().join("artifacts"))?;
        let timing = FrameTiming::frames_per_second(30)?;
        let first = RgbaImage::from_pixel(32, 24, Rgba([18, 24, 34, 255]));
        let mut recorder = LiveRecorder::start(&store, "live.mp4", false, timing, first)?;
        for red in [40, 80, 120, 160] {
            recorder.push(RgbaImage::from_pixel(32, 24, Rgba([red, 28, 48, 255])))?;
        }
        let artifact = recorder.finish()?;
        assert_eq!(artifact.frame_count, 5);
        assert_eq!(artifact.duration_ms, 166);
        let file = fs::File::open(&artifact.path).map_err(|error| error.to_string())?;
        let mp4 = read_mp4(file).map_err(|error| error.to_string())?;
        let track = mp4
            .tracks()
            .values()
            .find(|track| matches!(track.media_type(), Ok(MediaType::H264)))
            .ok_or_else(|| "missing H.264 track".to_owned())?;
        assert_eq!(track.sample_count(), 5);
        Ok(())
    }
}
