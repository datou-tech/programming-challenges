import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Class that prints out all the unique permutations of the string passed in
 * @author Eric Chu <eric.chu@gmail.com>
 */
public class StringPermutation
{

	// default shards to 1
	private static int shardLength = 1;
	// number of shards if we need to shard
	private static final int NUM_SHARDS = 6;
	// default length of the string to permutate before sharding is necessary
	private static final int DEFAULT_SHARD_LENGTH = 8;
	// the prefix for the shards for easy idenfication in the file system
	private static final String SHARD_PREFIX = "SHARD_";
	// caches all the references to the writers
	private static Set<String> shardedWriters = new HashSet<String>();

	/**
	 * Main method - accepts 1 string as input
	 */
    public static void main (String[] args)
    {

		// assert that user has entered a string of at least length 1
		if (args.length != 1)
		{
	    	System.out.println("Usage: Enter 1 string");
	    	System.out.println("Example: java StringPermutation permutateme");
			System.exit(1);
		}
		
		// entered string
		String permutateMe = args[0]; // MEM: 2 bytes per char
		
		// depending on how large the word is, we may need to shard the files
		if (permutateMe.length()>DEFAULT_SHARD_LENGTH)
		{
			// hash the set of shards on the first 5 characters of the permutation
			// TODO optimize this so that the shardLength can increase if needed
			shardLength=NUM_SHARDS;
		}
		
		try 
		{
			System.out.println("Starting permutation of string:"+permutateMe);
			long start = System.currentTimeMillis();

			// do the work
			permutation("", permutateMe);

			// merge and cleanup
			mergeShards(permutateMe);

			System.out.println("Finished generating permutations in " + (System.currentTimeMillis() - start) + "ms.");
		} 
		catch (Exception e)
		{
			System.out.println("Error running permutations.");
			e.printStackTrace();
		}

    }

	/**
 	* Recursive method runs through the permutations of a given string.
 	* @param permuted the part of the string that is held static so the rest of the is variably permutated.
	* @param permutateMe the string to be permutated
 	*/
	private static void permutation (String permuted, String permutateMe)
		throws Exception
	{
		int len = permutateMe.length();
		// if the size of the string is 0, 
		// then the prefix is one of the permutations 
        if (len == 0)
		{
			// check if this permutation has been seen already
			if (!exists(permuted))
			{
				// if it hasn't, then write it to one of the shards
				write(permuted);
			}
		}
        else 
		{
			// recursion
            for(int i = 0; i < len; i++)
			{
				// keep chopping off values from left to right in this recursion until we reach the (len==0) clause above to reach end of recursion
				permutation(permuted + permutateMe.charAt(i), permutateMe.substring(0, i) + permutateMe.substring(i+1, len));
            }
        }
	}

	/**
	 * Checks whether a string exists in a temporary file this class writes to
	 * @param permutation the permutation of the original string
	 * @return boolean value for if the string exists in the file
	 */
	private static boolean exists (String permutation)
		throws Exception
	{
		BufferedReader br = null;
		try 
		{
			// get the name of the sharded file
			String shardedFilename = getShardedFilename(permutation);
			// load file
			File file = new File(shardedFilename);
			// if the file exists
			if (file.exists())
			{
				br = new BufferedReader(new FileReader(file));
				String line = null;
				while ((line=br.readLine())!=null)
				{
					// and if the permutation already exists
					if (permutation.equals(line))
					{
						return true;
					}
				}
			}
		} 
		catch (Exception e)
		{
			System.out.println("Error occurred while trying to read sharded file.");
			e.printStackTrace();
		}
		finally
		{
			try { br.close(); } catch (Exception e) {}
		}
		
		return false;
	}
	
	/**
	 * Writes the string to a temporary file
	 * @param permutation the permutation of the original string
	 */
	private static void write (String permutation)
		throws Exception
	{
		Writer bw = null;
		String filename = getShardedFilename(permutation);

		File f = new File(filename);
		// if the file does not exist, save it
		if (!f.exists())
		{
			// create new file
			f.createNewFile();
			shardedWriters.add(filename);
		}
		// load a new buffered writer 
		bw = new BufferedWriter(new FileWriter(f, true));	
		// write out the permutation
		bw.write(permutation);
		((BufferedWriter)bw).newLine();
		bw.flush();
		bw.close();
	}
	
	/**
	 * Convenience method to normalize the name of the sharded files for look up purposes
	 * @param permutation the value of this permutation
	 * @return the normalized name of the shard
	 */
	private static String getShardedFilename (String permutation)
	{
		// if this run needs shards 
		if (shardLength>1)
		{
			return SHARD_PREFIX+permutation.substring(0,shardLength);
		}
		// only requires 1 shard
		else
		{
			return SHARD_PREFIX;
		}
	}
	
	/**
	 * Takes all the shards and loads it into a large file. Also deletes processed shards
	 * @param permutateMe the value of the string to permutate
	 */
	private static void mergeShards (String permutateMe)
	{
		Iterator it = shardedWriters.iterator();
		String key = null;
		BufferedReader br = null;
		BufferedWriter bw = null;
		File readFile = null;
		
		try
		{
			// open new buffered writer and name it uniquely
			bw = new BufferedWriter(new FileWriter(new File("master_permutations_"+permutateMe+"_"+System.currentTimeMillis()+".txt"),true));
			// for each shard
			while (it.hasNext())
			{
				// the name of the shard (filename) for this iteration
				key = (String)it.next();
				readFile = new File(key);

				// load the file into a reader
				br = new BufferedReader(new FileReader(readFile));
				String line = null;
				// for each line
				while ((line=br.readLine())!=null)
				{
					// write the line into a master file
					bw.write(line);
					bw.newLine();
				}
				bw.flush();
				
				// close the reader
				br.close();
				// after reading this file, delete it
				readFile.delete();
			}
		}
		catch (Exception e)
		{
			System.out.println("Error merging shards. Sorry, looks like this script took a dump in your directory. Clean up with 'rm SHARD_*'");
			e.printStackTrace();
		}
		finally
		{
			try { br.close(); } catch (Exception e) {}
			try { bw.close(); } catch (Exception e) {}
		}
	}
	
}
