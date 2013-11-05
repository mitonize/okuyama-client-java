package mitonize.datastore.okuyama;

import java.io.IOException;

import mitonize.datastore.KeyValueConsistencyException;
import mitonize.datastore.OperationFailedException;
import mitonize.datastore.Pair;
import mitonize.datastore.VersionedValue;


/**
 * Okuyamaにアクセスするクライアントライブラリである。接続先管理やTCPコネクションプールも行う。
 * Okuyamaアクセス時は getConnection() を用いて接続オブジェクトを取得し、利用終了したら Connection.close()を呼ぶこと。
 */
public interface OkuyamaClient {

	/**
	 * 保存可能な最大サイズをMasterNodeへ問い合わせる.
	 * 
	 * @return 保存可能な最大サイズ
	 * @throws IOException 
	 */
	long initClient() throws IOException;

	/**
	 * Okuyamaに値を保存する。キー及び値は内部的に Base64エンコードされる。
	 * 
	 * @param key キー文字列。コントロール文字を含む場合は例外
	 * @param value 値オブジェクト。nullも指定可。
	 * @param tags タグ文字列の配列。未設定の場合はnullを指定。
	 * @param age 値の有効時間(秒)。0を指定すると無期限。
	 * @return 登録成功の場合は true
	 * @throws IOException 
	 * @throws OperationFailedException 
	 * @throws IllegalArgumentException キー文字列にコントロール文字を含む場合
	 */
	boolean setObjectValue(String key, Object value, String[] tags, long age) throws IOException, OperationFailedException;
	
	/**
	 * キーを指定してOkuyamaから値を取得する。
	 * 
	 * @param key キー文字列。コントロール文字を含む場合は例外
	 * @return 値オブジェクト。nullも指定可。
	 * @throws IOException 通信時の例外
	 * @throws ClassNotFoundException 保管されているオブジェクトのクラスが見つからない場合
	 * @throws OperationFailedException 
	 * @throws IllegalArgumentException キー文字列にコントロール文字を含む場合
	 */
	Object getObjectValue(String key) throws IOException, ClassNotFoundException, OperationFailedException;

	
	/**
	 * 指定されたタグが含まれるキー群を取得する。
	 * 
	 * @param tag タグ
	 * @param withDeletedKeys 削除済みキーも返す場合はtrue
	 * @return キーの配列
	 * @throws IOException 
	 * @throws OperationFailedException 
	 */
	String[] getTagKeys(String tag, boolean withDeletedKeys) throws IOException, OperationFailedException;

	/**
	 * タグを指定してOkuyamaから値を取得する。
	 * 
	 * @param tags タグ文字列の配列。未設定の場合はnullを指定。
	 * @return 値オブジェクト。nullも指定可。
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws IllegalArgumentException キー文字列にコントロール文字を含む場合
	 */
	Pair[] getPairsByTag(String tag) throws IOException, ClassNotFoundException;

	VersionedValue getObjectValueVersionCheck(String key) throws IOException, OperationFailedException, ClassNotFoundException;

	boolean setObjectValueVersionCheck(String key, Object value, String version,
			String[] tags, long age) throws IOException, OperationFailedException, KeyValueConsistencyException;
	
}
