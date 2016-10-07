using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IO
{
    public static class IOFDDic
    {
        /// <summary>
        /// Stimulate the FD in linux to make a relationship between fd and file, the detail shoule review the linux to check the table between these
        /// It is a shared message!!! Every thread should see this, so must guarantee the thread safe
        /// key is threadID,value is IOpath
        /// </summary>
        public static Dictionary<int, string> FDMapping = new Dictionary<int, string>();
    }
}
