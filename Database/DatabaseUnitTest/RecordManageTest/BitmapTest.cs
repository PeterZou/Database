using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Database.RecordManage;

namespace DatabaseUnitTest.RecordManageTest
{
    [TestClass]
    public class BitmapTest
    {
        [TestMethod]
        public void BitmapSet()
        {
            Bitmap b = new Bitmap(124);

            b.Reset();
            b.Set(90);
            bool flag = b.Test(90);
            flag = b.Test(91);

            Bitmap c = new Bitmap(b.bitArray, 124);
        }
    }
}
