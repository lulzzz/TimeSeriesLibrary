using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This static class is used to call the functions in lz4_64.dll or lz4_32.dll, which is coded in C
    /// and compiled for native Win32 or Win64.  LZ4 is a fast-executing compression and decompression
    /// algorithm for byte arrays.
    /// The lz4 DLL is based on BSD-licensed source code from http://lz4.github.io/lz4/.  
    /// </summary>
    static unsafe class LZ4
    {
        #region Dll-Imports
        // Because the DLL is accessed using the DllImport attribute on these functions, if any
        // test classes depend on the LZ4 class, then those classes will need to have the 
        // 'DeploymentItem' attribute.  This is because when Visual Studio runs tests, it copies
        // the assembly to a temporary folder, but it does not recognize that the unmanaged
        // lz4_64.dll is a dependency that needs to be copied into this temporary folder.
        // For more information, see:
        // http://social.msdn.microsoft.com/forums/en-US/vststest/thread/9ffe18d4-e1fa-4de4-845f-634843802bb9/

        private static class LZ4_32
        {
            [DllImport("lz4_32.dll", CallingConvention = CallingConvention.Cdecl)]
            public extern static int LZ4_compress_fast(byte* source, byte* dest,
                                          int inputSize, int maxOutputSize, int acceleration);
            [DllImport("lz4_32.dll", CallingConvention = CallingConvention.Cdecl)]
            public extern static int LZ4_decompress_safe(byte* source, byte* dest,
                                          int compressedSize, int maxDecompressedSize);
            [DllImport("lz4_32.dll", CallingConvention = CallingConvention.Cdecl)]
            public extern static int LZ4_compressBound(int inputSize);
        }
        private static class LZ4_64
        {
            [DllImport("lz4_64.dll", CallingConvention = CallingConvention.Cdecl)]
            public extern static int LZ4_compress_fast(byte* source, byte* dest,
                                          int inputSize, int maxOutputSize, int acceleration);
            [DllImport("lz4_64.dll", CallingConvention = CallingConvention.Cdecl)]
            public extern static int LZ4_decompress_safe(byte* source, byte* dest,
                                          int compressedSize, int maxDecompressedSize);
            [DllImport("lz4_64.dll", CallingConvention = CallingConvention.Cdecl)]
            public extern static int LZ4_compressBound(int inputSize);
        }
        // delegate types for the delegates that will call either the 32-bit or 64-bit
        // functions from DLL import that are declared above
        private delegate int lz4_compress_delegate_type(byte* source, byte* dest,
                                          int inputSize, int maxOutputSize, int acceleration);
        private delegate int lz4_decompress_delegate_type(byte* source, byte* dest,
                                          int compressedSize, int maxDecompressedSize);
        private delegate int lz4_get_max_size_delegate_type(int inputSize);

        // Delegates for calling either the 32-bit or 64-bit functions from DLL import
        private static lz4_compress_delegate_type lz4_compress;
        private static lz4_decompress_delegate_type lz4_decompress;
        private static lz4_get_max_size_delegate_type lz4_get_max_size;
        #endregion

        #region static constructor
        /// <summary>
        /// static constructor
        /// </summary>
        static LZ4()
        {
            // If this is a 64-bit process
            if (Environment.Is64BitProcess)
            {
                // assign the delegates to call the functions in the 64-bit DLL
                lz4_compress = LZ4_64.LZ4_compress_fast;
                lz4_decompress = LZ4_64.LZ4_decompress_safe;
                lz4_get_max_size = LZ4_64.LZ4_compressBound;
            }
            // If this is a 32-bit process
            else
            {
                // assign the delegates to call the functions in the 32-bit DLL
                lz4_compress = LZ4_32.LZ4_compress_fast;
                lz4_decompress = LZ4_32.LZ4_decompress_safe;
                lz4_get_max_size = LZ4_32.LZ4_compressBound;
            }
        }
        #endregion

        #region Methods
        /// <summary>
        /// Assigns a compressed verion of InputByteArray to OutputByteArray
        /// </summary>
        /// <param name="inputByteArray">The byte array that is to be compressed</param>
        /// <param name="outputByteArray">The byte array into which the compressed data
        /// is to be written.  This array must be allocated before calling the method, and
        /// it must be large enough to contain the compressed data.  Note that if the data
        /// in InputByteArray is highly incompressible (floating point data that uses its
        /// full precision range is a known example), then OutputByteArray may need to be
        /// slightly larger than the InputByteArray.  This method will resize OutputByteArray
        /// so that its allocated size is no larger than the data it contains.</param>
        /// <param name="accelerationLevel">A level that can be applied to make compression
        /// run faster, generally at the cost of larger output arrays.  According to the LZ4
        /// documentation, a larger value causes faster run time, although our experiments have
        /// shown the effect to be small to the point of being difficult to verify.</param>
        static public void Compress(byte[] inputByteArray, ref byte[] outputByteArray,
                            int accelerationLevel)
        {
            int outputByteArrayLength = 0;

            fixed (byte* inputPtr = inputByteArray, outputPtr = outputByteArray)
            {
                outputByteArrayLength = lz4_compress(inputPtr, outputPtr, 
                          inputByteArray.Length, outputByteArray.Length,
                          accelerationLevel);
            }
            Array.Resize<byte>(ref outputByteArray, outputByteArrayLength);
        }

        /// <summary>
        /// Assigns a decompressed version of InputByteArray to OutputByteArray
        /// </summary>
        /// <param name="inputByteArray">The byte array that is to be decompressed</param>
        /// <param name="outputByteArray">The byte array into which the decompressed data
        /// is to be written.  This array must be allocated before calling the method, and
        /// it must have exactly the length that is needed to contain the decompressed data.
        /// If the decompressed data is found to be any larger or smaller than the length of
        /// OutputByteArray, then this method throws an exception.</param>
        static public void Decompress(byte[] inputByteArray, byte[] outputByteArray)
        {
            int allowedOutputArrayLength = outputByteArray.Length;

            fixed (byte* inputPtr = inputByteArray, outputPtr = outputByteArray)
            {
                lz4_decompress(inputPtr, outputPtr, inputByteArray.Length, allowedOutputArrayLength);
            }
            if (allowedOutputArrayLength != outputByteArray.Length)
                throw new TSLibraryException(ErrCode.Enum.Compression_Error,
                        "The decompressed size of a time series byte array was incorrect.");
        }
        /// <summary>
        /// Returns the maximum byte array length that LZ4 compression may output in a "worst case" scenario
        /// (i.e., if the input data is not compressible). It is recommended that the output array be
        /// allocated at this size before compression.
        /// </summary>
        /// <param name="inputByteArrayLength">the length of the uncompressed input byte array</param>
        static public int GetMaxSize(int inputByteArrayLength)
        {
            return lz4_get_max_size(inputByteArrayLength);
        }
        #endregion

    }
}
