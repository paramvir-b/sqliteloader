#!/usr/bin/env python

# import modules used here -- sys is a very standard one
import sys
import hashlib
import os.path

class RandomSequenceOfUnique:

   m_index = 0
   m_intermediateOffset = 0

   def __init__(self, seedBase, seedOffset):
      self.m_index = self.permuteQPR(self.permuteQPR(seedBase) + 0x682f0161);
      self.m_intermediateOffset = self.permuteQPR(self.permuteQPR(seedOffset) + 0x46790905);

   def permuteQPR(self, x):
      prime = 4294967291;
      if x >= prime:
         return x;  # The 5 integers out of range are mapped to themselves.

      residue = (x * x) % prime
      if x <= prime / 2:
         return residue
      else:
         return prime - residue;

   def next(self):
      self.m_index= self.m_index + 1;
      return self.permuteQPR((self.permuteQPR(self.m_index) + self.m_intermediateOffset) ^ 0x5bf03635);


# Gather our code in a main() function
def main():
    # Command line args are in sys.argv[1], sys.argv[2] ...
    # sys.argv[0] is the script name itself and can be ignored
    workDir=sys.argv[1]
    templateFile=sys.argv[2]
    outputFileName=sys.argv[3]
    numCount=int(sys.argv[4])
    if sys.argv[5] == 'true':
        forceCreate=True
    else:
        forceCreate=False
     
    actNumCount=2**numCount;
    print 'workDir=', workDir
    print 'templateFile=', templateFile
    print 'outputFileName=', outputFileName
    print 'numCount=', numCount
    print 'actNumCount=', actNumCount
    print 'forceCreate=', forceCreate

    if forceCreate:
        print 'Removing ',outputFileName,' file'
        os.remove(outputFileName)


    if os.path.isfile(outputFileName):
        print 'Skipping as ', outputFileName, ' file already exists'
        return;

    r = RandomSequenceOfUnique(1, 2)

    
    f = open(templateFile, 'r')
    lines = f.readlines()

    outputFile = open(outputFileName, "w")

    for i in range(0, actNumCount):
        for line in lines:
            rn = r.next()
            outputFile.write(str(rn))
            outputFile.write('\t')
            outputFile.write(line)
      
#    for i in range(10):
#        print 'rn', rn , ' md5=', hashlib.sha224(str(rn)).hexdigest()
        

# Standard boilerplate to call the main() function to begin
# the program.
if __name__ == '__main__':
    main()

