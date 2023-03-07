use ChunkerImpl;

/// Array of 256 random 32-bit values.
/// Created using python as such:
///
/// ```python
/// import numpy as np
///
/// # Create an array of 256 random 32-bit values
/// arr = np.random.randint(0, 2**32, size=256, dtype=np.uint32)
///
/// # Convert the values to hexadecimal strings and print them
/// for value in arr:
///     hex_string = hex(value)[2:].zfill(8)
///     print("0x" + hex_string)
/// ```
pub static TABLE: [u32; 256] = [
    0xd3eb979d, 0xd6d83960, 0x3cd24090, 0x702e952e, 0x50c47f4a, 0xdb56d7b6, 0x1470b666, 0x917d698c,
    0x4b222ddb, 0x9e61352d, 0xb1bb53ca, 0x73e47b3b, 0x48bc9c75, 0xcd61fb37, 0x3f3b76ab, 0x4e703f12,
    0x8192bf76, 0x3b4bcfbf, 0xb1096934, 0x6dcd3b70, 0x3b62b5e9, 0xa3a3fd75, 0x3f4a0b40, 0x104f764c,
    0x3aa01a20, 0xa0f97902, 0x54d5407a, 0x1de7011a, 0xd17e58a1, 0x6705c00a, 0x26ada853, 0xfdfd5ed4,
    0x867d636f, 0x0a56b662, 0xe9cb1ee2, 0xe472c33a, 0x1a06c290, 0x73d90737, 0xa1885d47, 0x6b593527,
    0xd77ca484, 0xfd43ca59, 0xf6798265, 0x8473ce32, 0x1dfa57af, 0xa7a7f764, 0x034199bf, 0xd92df7c0,
    0xc336657d, 0xb1d3cece, 0x28000293, 0x0c665d67, 0x19b6e719, 0x0b248029, 0xabf92644, 0x528ef61c,
    0x90cc63dc, 0x41b9ddca, 0x6608ac26, 0xccedfa3e, 0x58428c9c, 0xc611c58b, 0xb45fe506, 0x47e8e9ab,
    0x9395f52a, 0x0b92fd50, 0x486bc906, 0x7c50e4b7, 0x74eefe1b, 0xf7a390f7, 0x759ce338, 0x8adcb273,
    0x4aff20df, 0x1b8a1de9, 0xfe5bc7f1, 0x0d9289df, 0x722989a7, 0x15a2b030, 0x808d6900, 0x37d16afd,
    0xe859a17d, 0x65e3a83a, 0x7f5231e4, 0xa8caf398, 0x5749d956, 0xa8b5edde, 0xc3f90d02, 0x8b5ab74e,
    0x115b85ba, 0x57a81d60, 0xc571ffd0, 0xa7b52ed0, 0x0de8ed32, 0xfdd83d5a, 0x53a853b2, 0x1275193f,
    0xceec15c6, 0xb348f31d, 0x5d91f26b, 0xfc4b8c3b, 0x3cc2c8d9, 0x2b613d1e, 0xb6e587eb, 0xb3e81eee,
    0x444af107, 0x06586733, 0x98147a42, 0xf1f0c4ba, 0x7d95c1f0, 0x050bd680, 0xc9f2f924, 0xe1279f28,
    0x73d9e2b2, 0xdbd34e1e, 0xaf520344, 0xdc99a1cb, 0xfd68fbcf, 0x9be753b0, 0x0d4c09d5, 0xbba7712f,
    0xca358455, 0xf50f43db, 0xbb069de7, 0xdc200aba, 0x6080521f, 0x4fd6ed4e, 0xacaa0eed, 0x6ab677a1,
    0xc26028f9, 0x6bba9c53, 0x5ce949d4, 0x21711186, 0xdc7aa7bb, 0x9845db08, 0x726bd0e0, 0xd73b6b0e,
    0x87615762, 0xbc61f58f, 0x99589c58, 0x7924edec, 0x0a9148d0, 0x0122f447, 0x1511018d, 0x95f2baa6,
    0xd94bef95, 0x8feb8737, 0x41e83af9, 0x67fed3af, 0xc195482e, 0xab54854a, 0x26645909, 0x66454394,
    0x016225a5, 0x96adc68b, 0x6a6eb020, 0x967a1788, 0xcc314db9, 0x4d27dd83, 0x0abffdca, 0x5dacf213,
    0x7d6d2126, 0x854090d4, 0x3f8dcb4f, 0xc3e23d4e, 0xd99303ae, 0x37ac2ffd, 0x4e14f338, 0x9305c22d,
    0xcebe04f9, 0xde0b5a05, 0x0bb4274f, 0x24ae495d, 0xfe4e59bd, 0x1d18166e, 0xd346dd0d, 0xbde388d9,
    0x5f18a658, 0xcf1ef53f, 0xe2b8ab87, 0x9e97a024, 0xda9c7313, 0x3319d136, 0x1961fb76, 0x261853d7,
    0x8e605cd6, 0x583cac11, 0x6a449a3f, 0xf39dd03a, 0x35a4007b, 0x6e752d1a, 0xe048f028, 0x31c256ab,
    0x93bb9d77, 0x14bb5521, 0x80d5ebf4, 0x68422ba7, 0x9f226cf1, 0xc0a82467, 0xa45b8002, 0x5e512680,
    0xfa57dd1c, 0x8083f360, 0xea8d15fa, 0x83ad9409, 0x881e0cc1, 0x8d4aa576, 0x53ef83c1, 0x160b1560,
    0x8d8ed76a, 0x39319339, 0xbe0e9994, 0xcf52f560, 0x15a2a853, 0x09f6d3ff, 0xbc8920a7, 0xd6c92114,
    0xd9f3360f, 0xe4f3f680, 0x12730d34, 0xcf104091, 0x21124198, 0x7ae8cada, 0xb194894d, 0xa58b3219,
    0x8a2f692d, 0x27549dd9, 0x97055806, 0x80e3f331, 0xe23a2323, 0x3e7eafc7, 0x0de30268, 0xe8ef2539,
    0xe8ba3247, 0x1866f211, 0xf7502a7b, 0x16633364, 0x5d10629c, 0xbd245d16, 0x713eac0d, 0xb6cd9109,
    0x401a19f1, 0x8714069d, 0xe50f2f08, 0x7b209708, 0x08951d28, 0x2fa47147, 0x3aa8b7db, 0x7f7bf5ac,
    0x8c493103, 0x85d3c670, 0xc3c70275, 0x7dbc72d6, 0x060a9752, 0x6e37c74e, 0xf773837f, 0xac8702b4,
];

/// A hasher that implements the Gear algorithm.
///
/// This algorithm uses a `u8->u32` lookup table, which it mixes into the hash for each input byte.
///
/// Source: Xia, Wen, et al. "Ddelta: A deduplication-inspired fast delta compression approach."
/// Performance Evaluation 79 (2014): 258-272.
/// PDF: https://cswxia.github.io/pub/DElta-PEVA-2014.pdf
#[derive(Debug, Clone)]
pub struct GearChunker {
    mask: u32,
    state: GearState,
}

impl GearChunker {
    /// Creates a new Gear hasher.
    /// The mask should have log2(target_chunk_size) most-significant 1-bits set.
    pub fn new(mask: u32) -> GearChunker {
        GearChunker {
            mask,
            state: Default::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct GearState {
    hash: u32,
    pos: usize,
}

impl GearState {
    fn reset(&mut self) {
        self.hash = 0;
        self.pos = 0
    }

    fn ingest(&mut self, b: u8) {
        self.hash = (self.hash << 1).wrapping_add(TABLE[b as usize]);
        self.pos += 1;
    }

    fn check_hash(&self, mask: u32) -> bool {
        self.hash & mask == 0
    }
}

impl ChunkerImpl for GearChunker {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        for (i, &b) in data.iter().enumerate() {
            self.state.ingest(b);

            if self.state.check_hash(self.mask) {
                return Some(i);
            }
        }

        None
    }

    fn reset(&mut self) {
        self.state.reset()
    }
}

/// A hasher that implements the Gear algorithm with normalized chunking modifications.
///
/// This algorithm uses a `u8->u32` lookup table, which it mixes into the hash for each input byte.
/// The normalized chunking modifications utilize two bitmasks to find chunk boundaries:
/// - Chunks smaller or equal in size to the target chunk size use a lower bit mask consisting of
///   more 1-bits.
/// - As soon as the target chunk size is reached, the upper (=smaller) bitmask is used.
/// This reduces chunk size variability, probably at the cost of deduplication.
///
/// Source: Xia, Wen, et al. "Ddelta: A deduplication-inspired fast delta compression approach."
/// Performance Evaluation 79 (2014): 258-272.
/// PDF: https://cswxia.github.io/pub/DElta-PEVA-2014.pdf
///
/// Source for the normalized chunking modifications: Xia, Wen, et al. "FastCDC: A fast and
/// efficient content-defined chunking approach for data deduplication." 2016 {USENIX} Annual
/// Technical Conference ({USENIX}{ATC} 16). 2016.
/// PDF: https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf
#[derive(Debug, Clone)]
pub struct NormalizedChunkingGearChunker {
    lower_mask: u32,
    upper_mask: u32,
    target_chunk_size: usize,
    state: GearState,
}

impl NormalizedChunkingGearChunker {
    /// Creates a new Gear hasher.
    /// The masks should have a number most-significant 1-bits set.
    /// The lower mask is the larger one, applied until target_chunk_size is reached.
    /// Afterwards, the smaller upper_mask is applied.
    pub fn new(
        lower_mask: u32,
        upper_mask: u32,
        target_chunk_size: usize,
    ) -> NormalizedChunkingGearChunker {
        NormalizedChunkingGearChunker {
            lower_mask,
            upper_mask,
            target_chunk_size,
            state: Default::default(),
        }
    }
}

impl ChunkerImpl for NormalizedChunkingGearChunker {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        for (i, &b) in data.iter().enumerate() {
            self.state.ingest(b);

            let mask = if self.state.pos <= self.target_chunk_size {
                self.lower_mask
            } else {
                self.upper_mask
            };

            if self.state.check_hash(mask) {
                return Some(i);
            }
        }

        None
    }

    fn reset(&mut self) {
        self.state.reset()
    }
}
