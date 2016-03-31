import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

// Class for combining a FileSystem and a Path. Used for the common path creation, copying files between file systems, deleting files, etc.
public class FileSystemPath{

    public FileSystem fs = null;
    public Path pathName = null;
    
    static final String FS_LOCAL = "file:///"; 
    static final String separator = "/"; 
    public FileSystemPath()
    {
    	
    }
    
    public FileSystemPath(String strPath, Configuration conf) throws IOException
    {
    	SetFSPath(strPath, conf);
    }
	
    // Setup both the file system and path. If the local file system is explicitly specified then remove because this will be implicit
    // with the FileSystem
    public void SetFSPath(String strPath, Configuration conf) throws IOException
    {
    	pathName = new Path(strPath);
		fs = pathName.getFileSystem(conf);
		if (strPath.startsWith(FS_LOCAL))
		{
			pathName = new Path(strPath.substring(FS_LOCAL.length()));
		}
		
		//pathName = fs.makeQualified(pathName);
    }
    
    // Explicitly set a local path
    public void SetFSLocalPath(String strPath) throws IOException
    {
    	Configuration conf = new Configuration();
    	SetFSLocalPath(strPath, conf);
    }
    
    public void SetFSLocalPath(String strPath, Configuration conf) throws IOException
    {
    	fs = FileSystem.getLocal(conf);
    	if (strPath.startsWith(FS_LOCAL))
		{
			strPath = strPath.substring(FS_LOCAL.length());
		}
    	
    	pathName = fs.makeQualified(new Path(strPath));
    }
    
    public String getName()
    {
    	if (pathName == null)
    	  return "";
    	
    	return pathName.getName();
    }
    
    
    public String getQualifiedName()
    {
    	if (pathName == null)
    	  return "";
    	
    	return makeQualified().getName();
    }
    
    public Path makeQualified()
    {
    	return fs.makeQualified(pathName);
    }
    
    // Get all of the file paths contained under a directory. Used to interate through a list of files for processing.
    private static List<String> getAllFilePath(Path filePath, FileSystem fs, boolean bOnlyFileNames) throws FileNotFoundException, IOException {
	    List<String> fileList = new ArrayList<String>();
	    FileStatus[] fileStatus = fs.listStatus(filePath);
	    
	    for (FileStatus fileStat : fileStatus) {
	        if (fileStat.isDirectory()) {
	            fileList.addAll(getAllFilePath(fileStat.getPath(), fs, bOnlyFileNames));
	        } else {
	        	String fileName = fileStat.getPath().toString(); 
	        	if (fileName.contains("~") == false)
	        	{
	        	if (bOnlyFileNames)
	        	   fileList.add(fileName.substring(fileName.lastIndexOf(FileSystemPath.separator) + 1));
	        	 else
	        	   fileList.add(fileName);
	        	}
	        }
	    }
	    return fileList;
	}
	
    // get all of the files on the current path.
	public List<String> getAllFilePath() throws FileNotFoundException, IOException {
		
		List<String> lstFilePath = getAllFilePath(pathName, fs, false);
		return lstFilePath;
	}
	
	// Get all of the relative files on the current path.
   public List<String> getAllRelativeFilePath() throws FileNotFoundException, IOException {
		
		List<String> lstFilePath = getAllFilePath(pathName, fs, false);
		String strName = pathName.getName();
		for (int i = 0; i < lstFilePath.size(); i++)
		  {
			String fileName = lstFilePath.get(i);
			fileName = fileName.substring(fileName.lastIndexOf(strName + FileSystemPath.separator) + strName.length() + 1, fileName.length());
			lstFilePath.set(i, fileName);
		  }
		
		return lstFilePath;
	}
   
   // Copy files from the current files system to another.
   public boolean CopyFilesTo(FileSystemPath fsp, boolean bDeleteSrc, boolean bOverwrite)
   {
	   boolean bFileCopied = true;
		  
		  try{
		  List<String> lstFilePath = getAllFilePath();
		
		  for (int i = 0; i < lstFilePath.size() && bFileCopied; i++)
		  {
			String inFile = lstFilePath.get(i);
		    Path srcFile = fs.makeQualified(new Path(inFile));
		    fs.setVerifyChecksum(false);
		    fsp.fs.setVerifyChecksum(false);
		    bFileCopied = FileUtil.copy(fs, srcFile, fsp.fs, fsp.pathName, bDeleteSrc, bOverwrite, fsp.fs.getConf());
		  }
		  
		  }
		  catch (IOException e)
			{ 
			  e.printStackTrace();
			  bFileCopied = false;	
			}
 
		  
	      return bFileCopied;
   }
   
// Copies all files from the source FileSystemPath to the destination directory. All files end up in top level of dstDir.
  
	public boolean CopyFilesTo(String dstDir, boolean bDeleteSrc, boolean bOverwrite, Configuration conf)
	{
		// Let file system api handle which file system they are located on.
		  
		  boolean bFileCopied = true;
		  try
		  {
		  FileSystemPath fsp = new FileSystemPath(dstDir, conf);
		  bFileCopied = CopyFilesTo(fsp, bDeleteSrc, bOverwrite);
	      }
	     catch (IOException e)
		 { 
		  e.printStackTrace();
		  bFileCopied = false;	
		 }
		  
	      return bFileCopied;
	}
	
	// Make the specified directory path
	public boolean makePath() throws FileNotFoundException, IOException
	{
		boolean bMakePath = fs.mkdirs(pathName);
		
		return bMakePath;
	}
	
	// Delete the current path
	public boolean deletePath()  throws FileNotFoundException, IOException
	{
		boolean bDeleted = false;
		// delete old files and all subdirectories.
		if (fs.exists(pathName))
		  bDeleted = fs.delete(pathName, true);
		else
		  bDeleted = true;
		
		return bDeleted;
	}
	
	// Empty the path of all files
	public boolean emptyPath()
	{
		boolean bEmpty = false;
		try
		{
	    bEmpty = deletePath();
		if (bEmpty)
		  bEmpty = makePath();  
		}
		catch (IOException e)
		{ 
			e.printStackTrace();
		};
		
	 return bEmpty;
	}
	
	// Create a stream on the current file system and path.
	public FSDataOutputStream create() throws FileNotFoundException, IOException
	{
		return fs.create(pathName);
	}
}
