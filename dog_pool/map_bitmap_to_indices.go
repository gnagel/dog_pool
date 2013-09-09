package dog_pool

//
// Decode the Bitmap to "ON" Bits
//
// Implemented inline:
// - Benchmark_MapBitmapToIndices_All_Off	  200000	     6394 ns/op
// - Benchmark_MapBitmapToIndices_All_On	   50000	     9078 ns/op
// - Benchmark_MapBitmapToIndices_Mix_On	   50000	    13389 ns/op
//
//
// Using with MapByteToIndices:
// - Benchmark_MapBitmapToIndices_All_Off	  200000	      8943 ns/op
// - Benchmark_MapBitmapToIndices_All_On	   50000	     42539 ns/op
// - Benchmark_MapBitmapToIndices_Mix_On	   50000	     56024 ns/op
//
//
func MapBitmapToIndices(bitmap []byte) []int64 {
	if len(bitmap) == 0 {
		return []int64{}
	}

	output := make([]int64, len(bitmap)*8)
	count := 0

	for i, byte_at := range bitmap {
		// Skip empty bytes
		if byte_at == 0x00 {
			continue
		}

		// All the bits are on!
		if byte_at == 0xFF {
			i_8 := int64(i * 8)
			j := 0

			// 0
			output[count+j] = i_8 + int64(j)
			j++

			// 1
			output[count+j] = i_8 + int64(j)
			j++

			// 2
			output[count+j] = i_8 + int64(j)
			j++

			// 3
			output[count+j] = i_8 + int64(j)
			j++

			// 4
			output[count+j] = i_8 + int64(j)
			j++

			// 5
			output[count+j] = i_8 + int64(j)
			j++

			// 6
			output[count+j] = i_8 + int64(j)
			j++

			// 7
			output[count+j] = i_8 + int64(j)
			j++

			count += j
			continue
		}

		// Perform BitShift operations and figure out which bits are on
		for j := 0; j < 8; j++ {
			//
			// WARNING!! We are counting from Left --> Right here!
			//
			mask := byte(1) << uint(7-j)

			position := i*8 + j

			if mask&(byte_at) != 0 {
				output[count] = int64(position)
				count++
			} else {
			}
		}
	}

	// Return the slice of live ids
	return output[0:count]
}

//
// Perform BitShift operations and figure out which bits are on
// Return a 0-7 length slice with the "ON" bits
//
func MapByteToIndices(byte_at byte) []int {
	//
	// WARNING: Handy for testing, but too slow for production
	//
	switch byte_at {
	case 0x00:
		return []int{}

	case 0xFF:
		return []int{0, 1, 2, 3, 4, 5, 6, 7}

	default:
	}

	output := make([]int, 8)
	count := 0

	//
	// WARNING!! We are counting from Left --> Right here!
	//
	for j := 0; j < 8; j++ {
		mask := byte(1) << uint(7-j)
		if mask&(byte_at) != 0 {
			output[count] = j
			count++
		}
	}

	// Return the slice of live ids
	return output[0:count]
}
