using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;

namespace Database.IndexManage
{
    public class Bitmap2
    {
        private int size;
        public char[] bitArray;

        public Bitmap2(int numBitArray)
        {
            size = numBitArray;
            bitArray = new char[numChars()];
        }

        public Bitmap2(char[] formerArray, int numBitArray)
        {
            size = numBitArray;
            bitArray = UnWrap(new string(formerArray));
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

        public char[] To_char_buf(int len)
        {
            if (len != numChars()) throw new Exception();
            return Wrap(bitArray).ToArray();
        }

        // Wrap the bitmap
        // From:7000,end:7fff
        // 压缩
        // 对每个char，附加上头0111，再通过UFTF8转换为单字符（不是字节！！！UTF8最多可能占用3个字节）文本
        private string Wrap(char[] array)
        {
            List<char> list = new List<char>();
            foreach (var a in array)
            {
                list.AddRange(ConvertIntToFixArray(a));
            }

            return WrapSav(list.ToArray());
        }

        private string WrapSav(char[] array)
        {
            List<char> list = new List<char>();
            int num = array.Length;
            if (num % 12 != 0) throw new Exception();
            num = array.Length / 12;
            string strTmp = new string(array);
            for (int i = 0; i < num; i++)
            {
                string str = strTmp.Substring(i * 12, 12);
                str = "0111" + str;
                // ASCII逆转换
                list.Add(GetUNum(str));
            }
            return new string(Encoding.UTF8.GetChars(Encoding.UTF8.GetBytes(list.ToArray())));
        }

        // 展开
        // unwrap:将头0111去掉，组成新的array
        private char[] UnWrap(string str)
        {
            List<char> charList = new List<char>();

            foreach (var c in str)
            {
                int a = c & '\u0fff';
                // 截取12位
                //char[] array = ConvertIntToFixArray(a);
                charList.Add((char)a);
            }

            return charList.ToArray();
        }

        private char[] ConvertIntToFixArray(int num)
        {
            char[] array = new char[12];
            if (num > 4095) throw new Exception();
            for (int i = 11; i >=0; i--)
            {
                // ASCII转换
                array[i] = (char)(num % 2 + 48);
                num = num / 2;
            }
            return array;
        }

        private char ConvertASCToByte(string str)
        {
            // 第一个字符为0
            if (str.Length != 15) throw new Exception();

            UInt32 num = GetUNum(str);

            return (char)num;
        }

        private static char GetUNum(string str)
        {
            UInt32 num = 0;
            for (int i = 1; i <= str.Length; i++)
            {
                if (str[i - 1] == '1')
                    num += (UInt32)(Math.Pow(2, str.Length - i));
            }

            return (char)num;
        }
    }
}
