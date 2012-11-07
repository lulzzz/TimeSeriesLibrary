using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace TimeSeriesLibrary
{
    static unsafe class LZFX
    {
        #region Dll-Imports
        [DllImport("lzfx.dll", CallingConvention = CallingConvention.Cdecl)]
        private extern static int lzfx_compress(void* ibuf, uint ilen,
                          void* obuf, uint *olen);
        [DllImport("lzfx.dll", CallingConvention = CallingConvention.Cdecl)]
        private extern static int lzfx_decompress(void* ibuf, uint ilen,
                            void* obuf, uint *olen);
        #endregion


        static public void Compress(byte[] InputByteArray, ref byte[] OutputByteArray)
        {
            uint uOutputByteArrayLength = Convert.ToUInt32(OutputByteArray.Length);

            fixed(void* inputPtr = InputByteArray, outputPtr = OutputByteArray)
            {
                lzfx_compress(inputPtr, Convert.ToUInt32(InputByteArray.Length),
                              outputPtr, &uOutputByteArrayLength);
            }
            int OutputByteArrayLength = Convert.ToInt32(uOutputByteArrayLength);
            Array.Resize<byte>(ref OutputByteArray, OutputByteArrayLength);
        }

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

    }
}
