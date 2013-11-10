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
	 * @throws OperationFailedException 
	 */
	long initClient() throws IOException, OperationFailedException;

	/**
	 * Okuyamaに値を保存する。キー及び値は内部的に Base64エンコードされる。
	 * 
	 * @param key キー文字列。コントロール文字を含む場合は例外
	 * @param value 値オブジェクト。nullも指定可。
	 * @param tags タグ文字列の配列。未設定の場合はnullを指定。
	 * @param age 値の有効時間(秒)。0を指定すると無期限。
	 * @return 登録成功の場合は true
	 * @throws IOException 通信エラーの場合
	 * @throws OperationFailedException 操作が成功しなかった場合
	 * @throws IllegalArgumentException キー文字列にコントロール文字を含む場合
	 */
	boolean setObjectValue(String key, Object value, String[] tags, long age) throws IOException, OperationFailedException;

	/**
	 * キーを指定してOkuyamaから値を取得する。
	 * シリアライズされたオブジェクトがクラスが見つからないなどの原因でデシリアライズできなかった場合は
	 * ClassNotFoundException をオブジェクトとして返す。
	 * 
	 * @param key キー文字列。コントロール文字を含む場合は例外
	 * @return 値オブジェクト。存在しない場合は null となる。デシリアライズできなかった場合は ClassNotFoundExceptionオブジェクト。
	 * @throws IOException 通信時の例外
	 * @throws OperationFailedException 操作が成功しなかった場合
	 * @throws IllegalArgumentException キー文字列にコントロール文字を含む場合
	 */
	Object getObjectValue(String key) throws IOException, OperationFailedException;

	/**
	 * キーを指定してOkuyamaから値を削除する。キーが存在した場合は削除前の値を返す。
	 * シリアライズされたオブジェクトがクラスが見つからないなどの原因でデシリアライズできなかった場合は
	 * ClassNotFoundException をオブジェクトとして返す。
	 * 
	 * @param key キー文字列。コントロール文字を含む場合は例外
	 * @return 削除前の値オブジェクト。存在しない場合は null となる。デシリアライズできなかった場合は ClassNotFoundExceptionオブジェクト。
	 * @throws IOException 通信時の例外
	 * @throws OperationFailedException 操作が成功しなかった場合
	 * @throws IllegalArgumentException キー文字列にコントロール文字を含む場合
	 */
	Object removeObjectValue(String key) throws IOException, OperationFailedException;

	/**
	 * 新たにOkuyamaに値を保存する。既に存在する場合は失敗する。
	 * キー及び値は内部的に Base64エンコードされる。
	 * 
	 * @param key キー文字列。コントロール文字を含む場合は例外
	 * @param value 値オブジェクト。nullも指定可。
	 * @param tags タグ文字列の配列。未設定の場合はnullを指定。
	 * @param age 値の有効時間(秒)。0を指定すると無期限。
	 * @return 登録成功の場合は true
	 * @throws IOException 通信エラーの場合
	 * @throws OperationFailedException 操作が成功しなかった場合
	 * @throws IllegalArgumentException キー文字列にコントロール文字を含む場合
	 */
	boolean addObjectValue(String key, Object value, String[] tags, long age) throws IOException, OperationFailedException;

	/**
	 * 指定されたタグが含まれるキー群を取得する。
	 * 
	 * @param tag タグ
	 * @param withDeletedKeys 削除済みキーも返す場合はtrue
	 * @return キーの配列
	 * @throws IOException 通信エラーの場合
	 * @throws OperationFailedException 操作が成功しなかった場合
	 */
	String[] getTagKeys(String tag, boolean withDeletedKeys) throws IOException, OperationFailedException;
	/**
	 * キーを指定してOkuyamaから値を取得する。
	 * シリアライズされたオブジェクトがクラスが見つからないなどの原因でデシリアライズできなかった場合は
	 * ClassNotFoundException をオブジェクトとして返す。
	 * 
	 * @param keys キー文字列(可変引数)。コントロール文字を含む場合は例外
	 * @return 値オブジェクトの配列。存在しない場合は null となる。デシリアライズできなかった場合は ClassNotFoundExceptionオブジェクト。
	 * @throws IOException 通信時の例外
	 * @throws OperationFailedException 操作が成功しなかった場合
	 * @throws IllegalArgumentException キー文字列にコントロール文字を含む場合
	 */
	Object[] getMultiObjectValues(String ... keys) throws IOException, OperationFailedException;

	/**
	 * タグを指定してOkuyamaから値を取得する。
	 * シリアライズされたオブジェクトがクラスが見つからないなどの原因でデシリアライズできなかった場合は
	 * ClassNotFoundException を値として返す。
	 * 
	 * @param tags タグ文字列の配列。未設定の場合はnullを指定。
	 * @return 値オブジェクト。nullも指定可。
	 * @throws IOException 
	 * @throws OperationFailedException 操作が成功しなかった場合
	 * @throws IllegalArgumentException キー文字列にコントロール文字を含む場合
	 */
	Pair[] getPairsByTag(String tag) throws IOException, OperationFailedException;

	/**
	 * キーを指定してOkuyamaから値とバージョン情報を取得する。
	 * シリアライズされたオブジェクトがクラスが見つからないなどの原因でデシリアライズできなかった場合は
	 * ClassNotFoundException をオブジェクトとして返す。
	 * 
	 * @param key キー文字列。コントロール文字を含む場合は例外
	 * @return バージョン情報と値オブジェクトのペア。キーが存在しない場合は null となる。デシリアライズできなかった場合は値として ClassNotFoundExceptionオブジェクトを設定。
	 * @throws IOException 通信時の例外
	 * @throws OperationFailedException 操作が成功しなかった場合
	 * @throws IllegalArgumentException キー文字列にコントロール文字を含む場合
	 */
	VersionedValue getObjectValueVersionCheck(String key) throws IOException, OperationFailedException;

	/**
	 * Okuyamaに値を保存する。キー及び値は内部的に Base64エンコードされる。
	 * 
	 * @param key キー文字列。コントロール文字を含む場合は例外
	 * @param value 値オブジェクト。nullも指定可。
	 * @param version バージョン文字列。getObjectValueVersionCheck で取得したものを指定する。
	 * @param tags タグ文字列の配列。未設定の場合はnullを指定。
	 * @param age 値の有効時間(秒)。0を指定すると無期限。
	 * @return 登録成功の場合は true
	 * @throws IOException 通信時の例外
	 * @throws OperationFailedException 操作が成功しなかった場合
	 * @throws KeyValueConsistencyException 既に更新されていて指定したバージョンと一致しない
	 * @throws IllegalArgumentException キー文字列にコントロール文字を含む場合
	 */
	boolean setObjectValueVersionCheck(String key, Object value, String version,
			String[] tags, long age) throws IOException, OperationFailedException, KeyValueConsistencyException;
	
}
