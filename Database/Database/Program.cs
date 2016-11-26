using System.Linq;
using System.Text;
using System;
using System.Collections.Generic;

namespace Database
{
    class Program
    {
        static void Main(string[] args)
        {
            char c = Program.ConvertASCToByte("111111111111111");
        }

        private static void Test()
        {
            char c1 = '\u7000'; // 高位
            char c2 = '\u7fff'; // 低位

            string s1 = new string(Encoding.UTF8.GetChars(Encoding.UTF8.GetBytes(new char[] { c1, c2 })));
            char[] array = Program.UnWrap(s1);
            Wrap(array);

            Console.WriteLine();
            Console.ReadKey();
        }

        // From:7000,end:7fff
        // 压缩
        // 对每个char，附加上头0111，再通过UFTF8转换为单字符（不是字节！！！UTF8最多可能占用3个字节）文本
        private static string Wrap(char[] array)
        {
            int num = array.Length;
            if (num % 12 != 0) throw new Exception();
            num = array.Length / 12;
            string strTmp = new string(array);
            for(int i=0;i<num;i++)
            {
                string str = strTmp.Substring(i * 12, 12);
                str = "0111" + str;
                // TODO
                // ASCII逆转换
            }
            //return new string(Encoding.UTF8.GetChars(Encoding.UTF8.GetBytes(new char[] { c1, c2 }))); ;
            return null;
        }

        // 展开
        // unwrap:将头0111去掉，组成新的array
        private static char[] UnWrap(string str)
        {
            List<char> charList = new List<char>();

            foreach (var c in str)
            {
                int a = c & '\u0fff';
                // 截取12位
                char[] array = Program.ConvertIntToFixArray(a);
                charList.AddRange(array);
            }

            return charList.ToArray();
        }

        private static char[] ConvertIntToFixArray(int num)
        {
            char[] array = new char[12];
            if (num > 4095) throw new Exception();
            for(int i=0;i<12;i++)
            {
                // ASCII转换
                array[i] = (char)(num % 2+48);
                num = num / 2;
            }
            return array;
        }

        private static char ConvertASCToByte(string str)
        {
            // 第一个字符为0
            if (str.Length != 15) throw new Exception();

            UInt32 num = GetUNum(str);

            return (char)num;
        }

        private static UInt32 GetUNum(string str)
        {
            UInt32 num = 0;
            for (int i = 1; i <= str.Length; i++)
            {
                if(str[i-1] == '1')
                    num += (UInt32)(Math.Pow(2, str.Length-i));
            }

            return num;
        }
    }
}
