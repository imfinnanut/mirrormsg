package watchimage

// Building blocks for other encoders that target the same watch formats.

func BuildPalette(g8 []byte, size int) []byte { return buildPalette(g8, size) }

func ToGColor8(r, g, b uint32) byte { return toGColor8(r, g, b) }

func G8ToRGB(b byte) (int, int, int) { return g8ToRGB(b) }

func RGBDist(ar, ag, ab, br, bg, bb int) int { return rgbDist(ar, ag, ab, br, bg, bb) }

func PackTightIdx(idx []byte, w, h int, depth BitDepth) ([]byte, int) {
	return packTightIdx(idx, w, h, depth)
}

func RowEqAt(a []byte, o1, o2, n int) bool { return rowEqAt(a, o1, o2, n) }

// DitherEnabled, ThumbCap and FullCap report the settings from SetConfig.
func DitherEnabled() bool { return cfgDither }

func ThumbCap() BitDepth { return cfgThumbCap }

func FullCap() BitDepth { return cfgFullCap }

// DefaultFullCap is FullCap before SetConfig changes it.
const DefaultFullCap = BitDepth(defaultFullCap)
