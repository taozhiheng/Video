#include "com_persist_util_tool_analysis_CalculatorImpl.h"
#include <map>
#include <string.h>
#include <stdlib.h>
#include <iostream>
//#include <opencv2/core/mat.hpp>


jstring charTojstring(JNIEnv* env, const char* pat)
{
    //定义java String类 strClass
    jclass strClass = (env)->FindClass("Ljava/lang/String;");
    //获取String(byte[],String)的构造器,用于将本地byte[]数组转换为一个新String
    jmethodID ctorID = (env)->GetMethodID(strClass, "<init>", "([BLjava/lang/String;)V");
    //建立byte数组
    jbyteArray bytes = (env)->NewByteArray(strlen(pat));
    //将char* 转换为byte数组
    (env)->SetByteArrayRegion(bytes, 0, strlen(pat), (jbyte*) pat);
    // 设置String, 保存语言类型,用于byte数组转换至String时的参数
    jstring encoding = (env)->NewStringUTF("GB2312");
    //将byte数组转换为java String,并输出
    return (jstring) (env)->NewObject(strClass, ctorID, bytes, encoding);
}

char* jstringToChar(JNIEnv* env, jstring jstr) {
    char* rtn = NULL;
    jclass clsstring = env->FindClass("java/lang/String");
    jstring strencode = env->NewStringUTF("GB2312");
    jmethodID mid = env->GetMethodID(clsstring, "getBytes", "(Ljava/lang/String;)[B");
    jbyteArray barr = (jbyteArray) env->CallObjectMethod(jstr, mid, strencode);
    jsize alen = env->GetArrayLength(barr);
    jbyte* ba = env->GetByteArrayElements(barr, JNI_FALSE);
    if (alen > 0) {
        rtn = (char*) malloc(alen + 1);
        memcpy(rtn, ba, alen);
        rtn[alen] = 0;
    }
    env->ReleaseByteArrayElements(barr, ba, 0);
    return rtn;
}

/*
 * Class:     com_persist_util_tool_analysis_CalculatorImpl
 * Method:    predict
 * Signature: (Ljava/util/List;ILjava/lang/String;Ljava/lang/String;II)Ljava/util/HashMap;
 */
JNIEXPORT jobject JNICALL Java_com_persist_util_tool_analysis_CalculatorImpl_predict
  (JNIEnv *env, jobject obj, jobject list, jint reset, jstring modelFile, jstring trainedFile, jint batchSize, jint gpuId)
{
    printf("enter ok\n");
    jclass list_class = env->GetObjectClass(list);
    jmethodID list_size = env->GetMethodID(list_class, "size", "()I");
    jint size = env->CallIntMethod(list, list_size);
    jmethodID list_get = env->GetMethodID(list_class, "get", "(I)Ljava/lang/Object;");

    //map<string, cv::Mat> images;
    jobject o;
    jclass info_class;
    jfieldID keyId, valueId, rowsId, colsId;
    jbyteArray array;
    jint len;
    jbyte* pByte;
    jstring key;
    char* keyData;
    std::string url;
    jint rows, cols;
    //cv::Mat* pMat;

    o = env->CallObjectMethod(list, list_get, 0);
    info_class = env->GetObjectClass(o);
    keyId = env->GetFieldID(info_class, "key", "Ljava/lang/String;");
    valueId = env->GetFieldID(info_class, "value", "[B");
    rowsId = env->GetFieldID(info_class, "rows", "I");
    colsId = env->GetFieldID(info_class, "cols", "I");

    printf("start read data\n");
    int i;
    for(i = 0; i < size; i++)
    {
        o = env->CallObjectMethod(list, list_get, i);
        //get key
        key = (jstring) env->GetObjectField(o, keyId);
        keyData = jstringToChar(env, key);

        //get rows and cols
        rows = env->GetIntField(o, rowsId);
        cols = env->GetIntField(o, colsId);

        //get value
        array = (jbyteArray)env->GetObjectField(o, valueId);
        len  = env->GetArrayLength(array);
        pByte = (jbyte*)malloc((len+1) * sizeof(jbyte));
        env->GetByteArrayRegion(array, 0, len, pByte);
        pByte[len] = 0;

        //pMat = new cv::Mat(rows, cols, CV_8UC3, pByte);
        //images[(string(keyData)] = *pMat;
        free(pByte);
        free(keyData);
    }
    //invoke a cpp method such as:
    //std::map<std::string, float> predict(std::map<std::string, cv::Mat> images)
    //to get a result
    printf("read data ok\n");
    //...
    //std::string model_file(jstringToChar(env, modelFile));
    //std::string trained_file(jstringToChar(env, trainedFile));

    //std::map<std::string, float> results = predict(images, reset, model_file, trained_file, batchSize, gpuId);

    std::map<std::string, float> results;
    jclass map_class = env->FindClass("Ljava/util/HashMap;");
    jmethodID map_construct = env->GetMethodID(map_class, "<init>", "()V");
    jobject map_obj = env->NewObject(map_class, map_construct);
    jmethodID map_put = env->GetMethodID(map_class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    std::string origin_url;
    char* char_url;
    jstring str_url;

    jclass float_class = env->FindClass("Ljava/lang/Float;");
    jmethodID float_valueOf = env->GetStaticMethodID(float_class,"valueOf", "(F)Ljava/lang/Float;");
    jobject float_value;
    float p;

    std::map<std::string, float>::iterator it;

    for(it = results.begin(); it != results.end(); it++)
    {
        origin_url = it->first;
        p = it->second;
        str_url = charTojstring(env, origin_url.c_str());
        float_value = env->CallObjectMethod(float_class, float_valueOf, p);
        env->CallObjectMethod(map_obj, map_put, str_url, float_value);
    }
    printf("exit");
    return map_obj;
}
