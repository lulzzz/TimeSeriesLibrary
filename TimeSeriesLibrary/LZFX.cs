using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This static class is used to call the functions in lzfx.dll, which is coded in C and compiled for
    /// native Win32.  LZFX is a fast-executing compression and decompression algorithm for byte arrays, 
    /// although it does not achieve great compression ratios.  The lzfx DLL is based on BSD-licensed source 
    /// code from http://lzfx.googlecode.com.  
    /// </summary>
    static unsafe class LZFX
    {
        #region Dll-Imports
        // Because the DLL is accessed using the DllImport attribute on these functions, if any
        // test classes depend on the LZFX class, then those classes will need to have the 
        // 'DeploymentItem' attribute.  This is because when Visual Studio runs tests, it copies
        // the assembly to a temporary folder, but it does not recognize that the unmanaged
        // lzfx.dll is a dependency that needs to be copied into this temporary folder.
        // For more information, see:
        // http://social.msdn.microsoft.com/forums/en-US/vststest/thread/9ffe18d4-e1fa-4de4-845f-634843802bb9/

        private static class LZFX32
        {
            [DllImport("lzfx.dll", CallingConvention = CallingConvention.Cdecl)]
            public extern static int lzfx_compress(void* ibuf, uint ilen,
                              void* obuf, uint* olen);
            [DllImport("lzfx.dll", CallingConvention = CallingConvention.Cdecl)]
            public extern static int lzfx_decompress(void* ibuf, uint ilen,
                                void* obuf, uint* olen);
        }
        private static class LZFX64
        {
            [DllImport("lzfx64.dll", CallingConvention = CallingConvention.Cdecl)]
            public extern static int lzfx_compress(void* ibuf, uint ilen,
                              void* obuf, uint* olen);
            [DllImport("lzfx64.dll", CallingConvention = CallingConvention.Cdecl)]
            public extern static int lzfx_decompress(void* ibuf, uint ilen,
                                void* obuf, uint* olen);
        }
        // delegate types for the delegates that will call either the 32-bit or 64-bit
        // functions from DLL import that are declared above
        private delegate int lzfx_compress_delegate_type(void* ibuf, uint ilen,
                          void* obuf, uint* olen);
        private delegate int lzfx_decompress_delegate_type(void* ibuf, uint ilen,
                            void* obuf, uint* olen);

        // Delegates for calling either the 32-bit or 64-bit functions from DLL import
        private static lzfx_compress_delegate_type lzfx_compress;
        private static lzfx_decompress_delegate_type lzfx_decompress;
        #endregion

        #region static constructor
        /// <summary>
        /// static constructor
        /// </summary>
        static LZFX()
        {
            // If this is a 64-bit process
            if (Environment.Is64BitProcess)
            {
                // assign the delegates to call the functions in the 64-bit DLL
                lzfx_compress = LZFX64.lzfx_compress;
                lzfx_decompress = LZFX64.lzfx_decompress;
            }
            // If this is a 32-bit process
            else
            {
                // assign the delegates to call the functions in the 32-bit DLL
                lzfx_compress = LZFX32.lzfx_compress;
                lzfx_decompress = LZFX32.lzfx_decompress;
            }
        }
        #endregion

        #region Methods
        /// <summary>
        /// Assigns a compressed verion of InputByteArray to OutputByteArray
        /// </summary>
        /// <param name="InputByteArray">The byte array that is to be compressed</param>
        /// <param name="OutputByteArray">The byte array into which the compressed data
        /// is to be written.  This array must be allocated before calling the method, and
        /// it must be large enough to contain the compressed data.  Note that if the data
        /// in InputByteArray is highly incompressible (floating point data that uses its
        /// full precision range is a known example), then OutputByteArray may need to be
        /// slightly larger than the InputByteArray.  This method will resize OutputByteArray
        /// so that its allocated size is no larger than the data it contains.</param>
        static public void Compress(byte[] InputByteArray, ref byte[] OutputByteArray)
        {
            uint uOutputByteArrayLength = Convert.ToUInt32(OutputByteArray.Length);

            fixed (void* inputPtr = InputByteArray, outputPtr = OutputByteArray)
            {
                lzfx_compress(inputPtr, Convert.ToUInt32(InputByteArray.Length),
                              outputPtr, &uOutputByteArrayLength);
            }
            int OutputByteArrayLength = Convert.ToInt32(uOutputByteArrayLength);
            Array.Resize<byte>(ref OutputByteArray, OutputByteArrayLength);
        }

        /// <summary>
        /// Assigns a decompressed version of InputByteArray to OutputByteArray
        /// </summary>
        /// <param name="InputByteArray">The byte array that is to be decompressed</param>
        /// <param name="OutputByteArray">The byte array into which the decompressed data
        /// is to be written.  This array must be allocated before calling the method, and
        /// it must have exactly the length that is needed to contain the decompressed data.
        /// If the decompressed data is found to be any larger or smaller than the length of
        /// OutputByteArray, then this method throws an exception.</param>
        static public void Decompress(byte[] InputByteArray, byte[] OutputByteArray)
        {
            uint uOutputByteArrayLength = Convert.ToUInt32(OutputByteArray.Length);

            fixed (void* inputPtr = InputByteArray, outputPtr = OutputByteArray)
            {
                lzfx_decompress(inputPtr, Convert.ToUInt32(InputByteArray.Length),
                              outputPtr, &uOutputByteArrayLength);
            }
            int OutputByteArrayLength = Convert.ToInt32(uOutputByteArrayLength);
            if (OutputByteArrayLength != OutputByteArray.Length)
                throw new TSLibraryException(ErrCode.Enum.Compression_Error,
                        "The decompressed size of a time series byte array was incorrect.");
        } 
        #endregion

    }
}
