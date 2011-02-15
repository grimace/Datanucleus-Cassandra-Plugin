package com.xyz.inheritance;

import com.spidertracks.datanucleus.CassandraTest;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;

import com.eaio.uuid.UUID;
import com.xyz.inheritance.media.Asset;
import com.xyz.inheritance.media.Audio;
import com.xyz.inheritance.media.Image;
import com.xyz.inheritance.media.Note;
import com.xyz.inheritance.media.Video;


public class InheritanceTest extends CassandraTest {

	final int NUM_ASSETS = 2000;
	int assetCount=0, noteCount=0, imageCount=0, audioCount=0, videoCount = 0;
	
    @Test
 	public void testCreateAssets() {
		UUID testUUID = new UUID();
		String baseId = testUUID.toString();
		Random random = new Random();	// use a random number to get relative distribution of class types
		
        PersistenceManager pm = null;
        Transaction tx= null;
        System.out.println("XYZ InheritanceTest creating "+NUM_ASSETS+" Assets.");
        try
        {
            pm = pmf.getPersistenceManager();
            tx = pm.currentTransaction();
            tx.begin();
			Asset asset = null;
			for (int i=0; i < NUM_ASSETS; i++) {
				int bla = random.nextInt(5);
	 			switch (bla) {
	 			case 1:
	 				Note note = new Note();
	 				note.setName("Note "+baseId);
	 				note.setText("text : "+i);
	 				asset = note;
	 				noteCount++;
	 				break;
	 				
	 			case 2:
	 				Image image = new Image();
	 				image.setName("Image "+baseId);
	 				image.setImageType("image/jpeg");
	 				asset = image;
	 				imageCount++;
	 				break;
	 				
	 			case 3:
	 				Audio audio = new Audio();
	 				audio.setName("Audio "+baseId);
	 				audio.setAudioType("audio/x-mpeg3");
	 				asset = audio;
	 				audioCount++;
	 				break;
	 				
	 			case 4:
	 				Video video = new Video();
	 				video.setName("Video "+baseId);
	 				video.setVideoType("video/x-mpeg4");
	 				asset = video;
	 				videoCount++;
	 				break;
	 				
	 			case 0:
	 			default:
	 				asset = new Asset();
	 				asset.setName("Asset "+baseId);
	 				assetCount++;
	 				break;
	 			}
				pmf.getPersistenceManager().makePersistent(asset);
				
			}
            tx.commit();
        } catch (Exception e) {
        	e.printStackTrace();
        }
        finally
        {
            if (tx.isActive())
            {
                tx.rollback();
            }
            pm.close();
        }
		System.out.println("persisted assets : "+assetCount+" , notes : "+noteCount+" , images : "+imageCount+" , audio : "+audioCount+" videos : "+videoCount);
		assertTrue(NUM_ASSETS == (assetCount+noteCount+imageCount+audioCount+videoCount));
	}

    @SuppressWarnings("unchecked")
    @Test
	public void testLoadAssets() {

        // Perform some query operations
        PersistenceManager pm = null;
        Transaction tx= null;
        int loaded = 0;
        try
        {
            pm = pmf.getPersistenceManager();
            tx = pm.currentTransaction();
            tx.begin();
            System.out.println("Executing Query for Assets");
            Query query = pm.newQuery(Asset.class);
            query.setOrdering("name");
            query.setRange(0,2000);
            List<Asset> c = (List<Asset>)query.execute();
            if (c != null) {
                loaded = c.size();
            }
            tx.commit();
        } catch (Exception e) {
        	e.printStackTrace();
        }
        finally
        {
            if (tx.isActive())
            {
                tx.rollback();
            }
            pm.close();
        }
        System.out.println("testLoadAssets() found : "+loaded);
        assertTrue(NUM_ASSETS == loaded);
	}



    @SuppressWarnings("unchecked")
    @Test
	public void testLoadVideos() {

        // Perform some query operations
        PersistenceManager pm = null;
        Transaction tx= null;
        int videosLoaded = 0;
        try
        {
            pm = pmf.getPersistenceManager();
            tx = pm.currentTransaction();
            tx.begin();
            System.out.println("Executing Query for Videos");
            Query query = pm.newQuery(Video.class);
            query.setOrdering("name");
            query.setRange(0,2000);
            List<Asset> c = (List<Asset>)query.execute();
            if (c != null) {
            	videosLoaded = c.size();
            }
            tx.commit();
        } catch (Exception e) {
        	e.printStackTrace();
        }
        finally
        {
            if (tx.isActive())
            {
                tx.rollback();
            }
            pm.close();
        }
        System.out.println("testLoadVideos() found : "+videosLoaded);
        assertTrue(videosLoaded > 0);
	
	}

    @SuppressWarnings("unchecked")
    @Test
	public void testLoadAndDeleteAssets() {

        // Perform some query operations
        PersistenceManager pm = null;
        List<Asset> assetList = new ArrayList<Asset>();
        Transaction tx= null;
        int  totalDeleted = 0;
        long totalFound = 0;
        long resultCount = 0;
        long idx = 0;
        long result_size = 5000;
        
        try
        {
            pm = pmf.getPersistenceManager();
            tx = pm.currentTransaction();
            tx.begin();
            
            System.out.println("Executing Query for range of Assets");
            do {
                Query query = pm.newQuery(Asset.class);
                query.setOrdering("name");
                query.setRange(idx, idx+result_size);
    			List<Asset> c = (List<Asset>)query.execute();
                for (Asset asset : c) {
                	assetList.add(asset);
                }
    			totalFound += c.size();
                resultCount = c.size();
                
            } while (resultCount == result_size);
            for (Asset asset : assetList) {
            	pm.deletePersistent(asset);
            	totalDeleted++;
            }
            tx.commit();
        } catch (Exception e) {
        	e.printStackTrace();
        }
        finally
        {
            if (tx.isActive())
            {
                tx.rollback();
            }
            pm.close();
        }
        assertTrue(NUM_ASSETS == totalDeleted);
	
	}


}
