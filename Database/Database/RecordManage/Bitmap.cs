using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;

namespace Database.RecordManage
{
    public class Bitmap
    {
        private int size;
        public char[] bitArray;

        public Bitmap(int numBitArray)
        {
            size = numBitArray;
            bitArray = new char[numChars()];
        }

        public Bitmap(char[] formerArray, int numBitArray)
        {
            size = numBitArray;
            bitArray = new char[numChars()];
            for (int i = 0; i < formerArray.Length; i++)
                bitArray[i] = formerArray[i];
        }

        public int numChars()
        {
            int numChars = (size / ConstProperty.Char_Num);
            if ((size % ConstProperty.Char_Num) != 0)
                numChars++;
            return numChars;
        }

        public static int numChars(int sizePara)
        {
            int numChars = (sizePara / ConstProperty.Char_Num);
            if ((sizePara % ConstProperty.Char_Num) != 0)
                numChars++;
            return numChars;
        }

        public void Reset(UInt32 bitNum)
        {
            int location = (int)bitNum / ConstProperty.Char_Num;
            int offset = (int)bitNum % ConstProperty.Char_Num;

            int num = bitArray[location];
            bitArray[location] = (char)(num & ~(1 << offset));
        }

        public void Reset()
        {
            for (UInt32 i = 0; i < size; i++)
            {
                Reset(i);
            }
        }

        /// <summary>
        /// 1 is free and 0 is uesd
        /// </summary>
        /// <param name="bitNum"></param>
        public void Set(UInt32 bitNum)
        {
            int location = (int)bitNum / ConstProperty.Char_Num;
            int offset = (int)bitNum % ConstProperty.Char_Num;

            int num = bitArray[location];
            bitArray[location] = (char)(num | (1 << offset));
        }

        public void Set()
        {
            for (UInt32 i = 0; i < size; i++)
            {
                Set(i);
            }
        }

        // true if Empty
        public bool Test(UInt32 bitNum)
        {
            int location = (int)bitNum / ConstProperty.Char_Num;
            int offset = (int)bitNum % ConstProperty.Char_Num;
            int num = bitArray[location];
            return ((num & (1 << offset)) == 0);
        }

        public void To_char_buf(char[] b, int len)
        {
            if (b == null || len != numChars()) throw new Exception();
            for (int i = 0; i < len; i++)
            {
                b[i] = bitArray[i];
            }
        }
    }
}
