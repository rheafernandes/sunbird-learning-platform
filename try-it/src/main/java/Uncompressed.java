import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Uncompressed {
    public static void main(String[] args) {
        try {
            Path folder = Paths.get("/data/logs");
            long size = Files.walk(folder)
                    .filter(p -> p.toFile().isFile())
                    .mapToLong(p -> p.toFile().length())
                    .sum();
            System.out.println(size);
            File fold = new File("/data/logs");
            long siz = FileUtils.sizeOfDirectory(fold);
            System.out.println(siz);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
