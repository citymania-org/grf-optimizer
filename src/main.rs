use std::borrow::Cow;
use std::fs::File;
use std::path::Path;
use std::io::{Read, Seek, Write, BufWriter, SeekFrom};
use std::time::{Duration, Instant};

use clap::Parser;
use byteorder::{WriteBytesExt, LittleEndian};



fn buf_max_overlap(d: &[u8], max_overlap: &mut Vec<u8>, overlap_ofs: &mut Vec<u16>) {
    let n: usize = d.len();
    let mut prev = vec![0usize; 256];
    let mut buf = vec![0usize; n + 1];

    for i in 0..n {
        buf[i + 1] = prev[d[i] as usize];
        prev[d[i] as usize] = i + 1;
    }

    for l in 2..(17.min(n) as u8) {
        for pos in (1usize..n).rev() {
            buf[pos + 1] = 0;
            let start = pos.saturating_sub(1 << 11);
            let mut i = buf[pos];
            while i > start && d[i] != d[pos] {
                i = buf[i];
            }
            if i > start {
                buf[pos + 1] = i + 1;
                if l > 2 {
                    max_overlap[pos] = l;
                    overlap_ofs[pos] = (pos - i) as u16;
                }
            }
        }
    }
}


fn encode_sprite_v4(d: &Vec<u8>) -> Cow<[u8]> {
    let n = d.len();

    if n <= 3 {
        let mut res = Vec::<u8>::with_capacity(n + 1);
        res.push(n as u8);
        res.extend(d);
        return Cow::Owned(res);
    }

    let mut max_overlap = vec![0u8; n];
    let mut overlap_ofs = vec![0u16; n];

    buf_max_overlap(&d, &mut max_overlap, &mut overlap_ofs);

    let mut open_len = vec![1u8; n];
    let mut optimal = vec![0usize; n];
    let mut prev = vec![-1isize; n];
    open_len[0] = 1;
    optimal[0] = 2;
    prev[0] = -1;
    for i in 1..n {
        optimal[i] = optimal[i - 1] + 2;
        let nl = open_len[i - 1] as usize + 1;
        let m = nl + 1 + if nl <= i { optimal[i - nl] } else { 0 };
        if m < optimal[i] {
            optimal[i] = m;
            prev[i] = -(nl as isize);
            if nl < 0x80 {
                open_len[i] = nl as u8;
            }
        }

        for j in 3..=max_overlap[i] {
            let m = optimal[i - j as usize] + 2;
            if m < optimal[i] {
                optimal[i] = m;
                prev[i] = j as isize;
            }
        }
    }

    let mut res = vec![0u8; optimal[n - 1]];
    let mut pos = optimal[n - 1];
    let mut i = n as isize - 1;

    while i >= 0 {
        let l = prev[i as usize];
        if l < 0 {
            for j in 0..-l {
                pos -= 1;
                res[pos] = d[(i - j) as usize];
            }
            pos -= 1;
            res[pos] = ((-l) & 0x7f) as u8;
            i += l;
        } else {
            pos -= 1;
            res[pos] = (overlap_ofs[i as usize] & 0xFF) as u8;
            pos -= 1;
            res[pos] = 0x80 | ((16 - l as u8) << 3) | (overlap_ofs[i as usize] >> 8) as u8;
            i -= l;
        }
    }

    Cow::Owned(res)
}


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg()]
    input: String,

    /// Number of times to greet
    #[arg()]
    output: String,
}

const BUF_SIZE: usize = 1 << 13;

struct BufRW {
    input_file: File,
    // output_file: File,
    // input: BufReader;
    output: BufWriter<File>,
    input_buf: [u8; BUF_SIZE],
    // output_buf: Vec<u8>,
    input_pos: u64,
    input_buf_pos: usize,
    input_buf_size: usize,
    input_buf_requested: u64,
    output_pos: usize,
    copy_mode: bool,
    // output_buf_pos: u64,
}

impl BufRW {
    pub fn new(input_file: File, output_file: File) -> Self {
        Self {
            input_file,
            // output_file,
            input_buf: [0u8; BUF_SIZE],
            output: BufWriter::new(output_file),
            input_pos: 0,
            input_buf_pos: 0,
            input_buf_size: 0,
            input_buf_requested: 0,
            output_pos: 0,
            copy_mode: true,
            // output_buf_pos: 0,
        }
    }

    pub fn set_copy_mode(&mut self, mode: bool) {
        self.copy_mode = mode;
    }

    pub fn request_buf(&mut self, size: u64) {
        self.input_buf_requested = if size >= BUF_SIZE as u64 { 0 } else { size };
    }

    pub fn get_byte(&mut self) -> u8 {
        if self.input_buf_pos >= self.input_buf_size {
            if self.input_buf_requested != 0 {
                self.input_buf_size = std::io::Read::by_ref(&mut self.input_file).take(self.input_buf_requested).read(&mut self.input_buf).expect("unexpected end of file");
                self.input_buf_requested -= self.input_buf_size as u64;
            } else {
                self.input_buf_size = self.input_file.read(&mut self.input_buf).expect("unexpected end of file");
            }
            if self.input_buf_size == 0 { panic!("unexpected end of file"); }
            self.input_buf_pos = 0;
        }
        let value = self.input_buf[self.input_buf_pos];
        self.input_buf_pos += 1;
        self.input_pos += 1;
        if self.input_buf_requested > 0 { self.input_buf_requested -= 1; }
        if self.copy_mode {
            self.put_byte(value);
        }
        return value;
    }

    pub fn get_word(&mut self) -> u16 {
        let b1 = self.get_byte() as u16;
        let b2 = self.get_byte() as u16;
        return b1 | (b2 << 8);
    }

    pub fn get_dword(&mut self) -> u32 {
        let b1 = self.get_byte() as u32;
        let b2 = self.get_byte() as u32;
        let b3 = self.get_byte() as u32;
        let b4 = self.get_byte() as u32;
        return b1 | (b2 << 8) | (b3 << 16) | (b4 << 24);
    }

    fn flush(&mut self) {
        // TODO switch to fix mode
        self.output.flush();
    }

    fn put_byte(&mut self, value: u8) {
        self.output.write_u8(value).unwrap();
    }

    fn put_word(&mut self, value: u16) {
        self.output.write_u16::<LittleEndian>(value).unwrap();
    }

    fn put_dword(&mut self, value: u32) {
        self.output.write_u32::<LittleEndian>(value).unwrap();
    }

    fn put_data(&mut self, data: &[u8]) {
        for b in data {
            self.put_byte(*b);
        }
    }

    pub fn copy(&mut self, n: usize) {
        // TODO check copy mode
        for _ in 0..n {
            self.get_byte();
        }
    }

    pub fn tell(&self) -> u64 { self.input_pos }

    pub fn seek(&mut self, position: u64) {
        if position == self.input_pos { return; }
        if position > self.input_pos {
            let skip = (position - self.input_pos) as usize;
            if skip + self.input_buf_pos <= self.input_buf_size {
                self.input_buf_pos += skip;
                self.input_pos += skip as u64;
                return;
            }
        }
        self.input_pos = self.input_file.seek(SeekFrom::Start(position)).unwrap();
        self.input_buf_size = 0;
        self.input_buf_requested = 0;
    }

    pub fn fix_dword(&mut self, position: u64, value: u32) {
        // TODO check fix mode
        self.output.get_mut().seek(SeekFrom::Start(position)).unwrap();
        self.output.get_mut().write_u32::<LittleEndian>(value).unwrap();
    }
}

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::{Hash, DefaultHasher, Hasher};

#[derive(Hash, Debug, Eq, PartialEq, Copy, Clone)]
struct SpriteID(u32);

#[derive(Hash)]
struct Sprite {
    zoom: u8,
    layers_mask: u8,
    width: u16,
    height: u16,
    xofs: u16,
    yofs: u16,
    decomp_size: u32,
    uses_tile_compression: bool,
    compressed_data: Vec<u8>,
    data: Vec<u8>,
    byte_size: usize,
}


fn decode_chunked(data: &Vec<u8>, width: u16, height: u16, bpp: u8, decomp_size: u32) -> Vec<u8> {
    let mut res = vec![0u8; width as usize * height as usize * bpp as usize];
    let mut offsets = Vec::<u32>::new();
    if decomp_size > 0xFFFF {
        for i in 0..height as usize {
            offsets.push(
                data[4 * i] as u32 |
                ((data[4 * i + 1] as u32) << 8) |
                ((data[4 * i + 2] as u32) << 16) |
                ((data[4 * i + 3] as u32) << 24)
            );
        }
    } else {
        for i in 0..height as usize {
            offsets.push(
                data[2 * i] as u32 |
                ((data[2 * i + 1] as u32) << 8)
            );
        }
    }
    // println!("ofsets: {:?}", offsets);

    for i in 0..height as usize {
        let mut ofs = offsets[i] as usize;
        let mut is_final = false;
        let mut length = 0u32;
        let mut skip = 0usize;
        // println!("y: {} ofs: {}", i, ofs);
        while !is_final {
            if width > 256 {
                is_final = data[ofs + 1] & 0x80 != 0;
                length =((data[ofs + 1] as u32 & 0x7f) << 8) | data[ofs] as u32;
                skip = (((data[ofs + 3] as u32) << 8) | data[ofs + 2] as u32) as usize;
                ofs += 4;
            } else {
                is_final = data[ofs] & 0x80 != 0;
                length = data[ofs] as u32 & 0x7f;
                skip = data[ofs + 1] as usize;
                ofs += 2;
            }
            let byte_length = length as usize * bpp as usize;
            let start = (i * width as usize + skip) * bpp as usize;
            // println!("is_final: {} length: {} skip: {}, start: {}+{} ofs: {}/{} - {}", is_final, length, skip, start, byte_length, ofs, data.len(), ofs + byte_length);
            res[start..start + byte_length].clone_from_slice(&data[ofs..ofs + byte_length]);
            ofs += byte_length;
        }
    }
    return res;
}

fn read_sprite(rw: &mut BufRW) -> Sprite {
    let sprite_start = rw.tell();
    let size = rw.get_dword();
    let t = rw.get_byte();
    let zoom = rw.get_byte();
    let height = rw.get_word();
    let width = rw.get_word();
    let xofs = rw.get_word();
    let yofs = rw.get_word();
    let mut bpp = 0;
    if t & 0x01 != 0 { bpp += 3; }  // RGB
    if t & 0x02 != 0 { bpp += 1; }  // A
    if t & 0x04 != 0 { bpp += 1; }  // M
    let uses_tile_compression = t & 0x08 != 0;

    let mut decomp_size = if uses_tile_compression { rw.get_dword() } else { width as u32 * height as u32 * bpp as u32 };

    // println!("Sprite t{} z{} {}x{} +{}+{} size={}({}) {}", t, zoom, width, height, xofs, yofs, decomp_size, uses_tile_compression, width as u32 * height as u32 * bpp as u32);

    let mut data = Vec::<u8>::with_capacity(decomp_size as usize);

    let mut compressed_data = Vec::<u8>::new();
    let mut bytes_left = decomp_size;

    let mut literal_len = 0u8;
    let mut pattern_byte = 0u8;
    while bytes_left > 0 {
        let b = rw.get_byte();
        compressed_data.push(b);
        if literal_len > 0 {
            data.push(b);
            literal_len -= 1;
            bytes_left -= 1;
        } else if pattern_byte > 0 {
            let ofs = (((pattern_byte & 7) as usize) << 8) | b as usize;
            let len = 16 - ((pattern_byte >> 3) & 15) as usize;
            if ofs > data.len() { panic!("lz77 offset is outside the buffer"); }
            if ofs == 0 { panic!("lz77 offset is zero"); }
            let pos = data.len() - ofs;
            for i in pos..pos + len { data.push(data[i]); }
            bytes_left -= len as u32;
            pattern_byte = 0;
        } else {
            if b >= 0x80 { pattern_byte = b; }
            else if b > 0 { literal_len = b; }
            else { literal_len = 0x80; }
        }
    }

    if uses_tile_compression {
        data = decode_chunked(&data, width, height, bpp, decomp_size);
    }

    Sprite {
        zoom,
        layers_mask: t & 0x7,
        width,
        height,
        xofs,
        yofs,
        decomp_size,
        compressed_data: compressed_data,
        uses_tile_compression: uses_tile_compression,
        data: data,
        byte_size: (rw.tell() - sprite_start) as usize,
    }
}


fn write_sprite(rw: &mut BufRW, sprite: &Sprite) -> usize {
    let encoded = encode_sprite_v4(&sprite.data);
    let encoded_size = encoded.len() + 14;
    let use_old = encoded_size >= sprite.byte_size;
    let use_tile_compression = use_old && sprite.uses_tile_compression;

    rw.put_dword((if use_old { sprite.byte_size} else { encoded_size } - 4) as u32);
    rw.put_byte(sprite.layers_mask | if use_tile_compression { 0x08 } else { 0 });
    rw.put_byte(sprite.zoom);
    rw.put_word(sprite.height);
    rw.put_word(sprite.width);
    rw.put_word(sprite.xofs);
    rw.put_word(sprite.yofs);
    if use_tile_compression {
        rw.put_dword(sprite.decomp_size);
    }
    if use_old {
        rw.put_data(sprite.compressed_data.as_slice());
    } else {
        rw.put_data(encoded.as_ref());
    }

    return if use_old { 0 } else { sprite.byte_size - encoded_size };
}


fn main() {
    let args = Args::parse();

    let input_path = Path::new(&args.input);
    let mut input = File::open(input_path).unwrap();
    let mut output = File::create(args.output).unwrap();
    let mut rw = BufRW::new(input, output);

    println!("Optimizing {}...", input_path.file_name().unwrap().to_str().unwrap());
    let start = Instant::now();

    let container = rw.get_byte();
    if container != 0 { panic!("Old grf container"); }
    rw.copy(9 + 5);

    let mut sprite_ref_ofs = HashMap::<SpriteID, Vec::<u64>>::new();

    println!("Processing pseudo sprites...");
    loop {
        let size = rw.get_dword();
        if size == 0 { break; }
        let grf_type = rw.get_byte();
        // println!("Size {} {}", size, grf_type);
        if grf_type != 0xFD {
            rw.copy(size as usize);
            continue;
        }
        let ofs = rw.tell();
        let sprite_id = SpriteID(rw.get_dword());
        sprite_ref_ofs.entry(sprite_id).or_default().push(ofs);
        // println!("Ref #{:?} at {}", sprite_id, ofs);
    }
    rw.set_copy_mode(false);

    println!("Loading real sprite offsets...");
    let mut sprite_ofs = HashMap::<SpriteID, Vec::<u64>>::new();
    loop {
        rw.request_buf(8);
        let sprite_id = SpriteID(rw.get_dword());
        if sprite_id == SpriteID(0) { break; }
        let ofs = rw.tell();
        sprite_ofs.entry(sprite_id).or_default().push(ofs);
        let size = rw.get_dword();
        // println!("Sprite #{:?} at {} size {}", sprite_id, ofs, size);
        rw.seek(ofs + size as u64 + 4);
    }

    let mut change_ref = Vec::<(SpriteID, SpriteID)>::new();
    let mut sprite_hashes = HashMap::<u64, SpriteID>::new();

    let mut total_sprites = 0usize;
    let mut dedup_bytes = 0usize;
    let mut dedup_collections = 0usize;
    let mut dedup_sprites = 0usize;
    let mut compression_bytes = 0usize;
    let mut compression_sprites = 0usize;

    println!("Processing real sprites...");
    for (sprite_id, &ref offsets) in &sprite_ofs {
        let mut sprites = Vec::<Sprite>::new();
        let mut hasher = DefaultHasher::new();
        let mut sprites_size = 0usize;
        for ofs in offsets {
            rw.seek(*ofs);
            let sprite = read_sprite(&mut rw);
            sprite.hash(&mut hasher);
            sprites_size += sprite.byte_size;
            sprites.push(sprite);
            total_sprites += 1;
        }
        let hash = hasher.finish();
        match sprite_hashes.entry(hash) {
            Entry::Vacant(entry) => {
                entry.insert(*sprite_id);
                for sprite in sprites {
                    rw.put_dword(sprite_id.0);
                    let res = write_sprite(&mut rw, &sprite);
                    if res > 0 {
                        compression_sprites += 1;
                        compression_bytes += res;
                    }
                }
            },
            Entry::Occupied(entry) => {
                change_ref.push((*sprite_id, *entry.get()));
                dedup_bytes += sprites_size;
                dedup_sprites += offsets.len();
                dedup_collections += 1;
            },
        };
    }
    rw.put_dword(0);
    rw.flush();

    println!("Fixing references for de-duplicated sprites...");
    for (sprite_id, orig_sprite_id) in change_ref {
        for position in sprite_ref_ofs.get(&sprite_id).unwrap() {
            rw.fix_dword(*position, orig_sprite_id.0);
        }
    }

    println!("Removed duplicates: {} sprite collections ({} sprites total), {} bytes.",
        dedup_sprites, dedup_collections, dedup_bytes);
    println!("Improved compression on {}/{} sprites, {} bytes saved",
        compression_sprites, total_sprites, compression_bytes);
    println!("Total {} bytes saved, processing time {:.2} seconds",
        compression_bytes + dedup_bytes, start.elapsed().as_secs_f32());
}
