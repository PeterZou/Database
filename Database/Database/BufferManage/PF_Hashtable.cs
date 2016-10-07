using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.BufferManage
{
    public class PF_Hashtable
    {
        private int numBuckets;

        private List<List<PF_HashEntry>> hashTableArray = new List<List<PF_HashEntry>>();

        public PF_Hashtable(int numBuckets)
        {
            this.numBuckets = numBuckets;
            for (int i = 0; i < this.numBuckets; i++)
            {
                hashTableArray.Add(new List<PF_HashEntry>());
            }
        }

        public int Found(int fd, int pageNum)
        {
            int bucket = Hash(fd, pageNum);

            if (hashTableArray == null || hashTableArray.Count <= bucket) throw new IndexOutOfRangeException();

            foreach (var h in hashTableArray[bucket])
            {
                if (h.pageNum == pageNum && h.fd == fd) return h.slot;
            }

            //Did not found
            return -1;
        }

        public void Insert(int fd, int pageNum, int slot)
        {
            int bucket = Hash(fd, pageNum);

            //Must not exist
            foreach (var h in hashTableArray[bucket])
            {
                if (h.pageNum == pageNum && h.fd == fd) throw new Exception();
            }

            PF_HashEntry node = new PF_HashEntry();
            node.fd = fd;
            node.pageNum = pageNum;
            //Slot is related to the buffer in the memory
            node.slot = slot;

            hashTableArray[bucket].Insert(0, node);
        }

        public void Delete(int fd, int pageNum)
        {
            int bucket = Hash(fd, pageNum);

            int index = -1;

            //Must exist
            foreach (var h in hashTableArray[bucket])
            {
                if (h.pageNum == pageNum && h.fd == fd)
                {
                    index = hashTableArray[bucket].IndexOf(h);
                    break;
                }
            }

            //Did not found it
            if (index == -1) throw new Exception();

            hashTableArray[bucket].RemoveAt(index);
        }

        private int Hash(int fd, int pageNum) { return ((fd + pageNum) % numBuckets); }
    }
}
