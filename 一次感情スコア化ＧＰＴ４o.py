import asyncio
import os
import pandas as pd
from openai import OpenAI
import customtkinter as ctk
from tkinter import filedialog, messagebox
import threading
import traceback

class ModerationTool:
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.df = None
        self.batch_size = 10
        self.max_tokens_per_request = 2048
        self.max_texts_per_request = 20
        self.temperature = 0.01
        self.top_p = 0.0

    async def moderate_text_batch(self, texts):
        if not texts:
            return []
            
        if len(texts) > self.max_texts_per_request:
            texts = texts[:self.max_texts_per_request]
            print(f"警告: テキスト数が制限を超えたため、最初の{self.max_texts_per_request}個のみを処理します")
        
        total_chars = sum(len(str(text)) for text in texts)
        if total_chars > self.max_tokens_per_request * 2:
            print("警告: トークン数が制限を超える可能性があります")
        
        try:
            response = await asyncio.to_thread(
                self.client.moderations.create,
                input=texts,
                model="omni-moderation-latest"
            )
            return response.results
        except Exception as e:
            print(f"モデレーションエラー: {e}")
            traceback.print_exc()
            return [None] * len(texts)

    async def get_aggressiveness_scores_batch(self, texts):
        if not texts:
            return []
            
        async def process_chunk(chunk):
            try:
                messages = [
                    {"role": "system", "content": "You are a helpful assistant that analyzes text for emotions."},
                    {"role": "user", "content": self._create_scoring_prompt(chunk)}
                ]
                
                response = await asyncio.to_thread(
                    self.client.chat.completions.create,
                    model="gpt-4o-2024-11-20",
                    messages=messages,
                    temperature=self.temperature,
                    top_p=self.top_p
                )
                
                scores_and_reasons = self._parse_batch_response(response.choices[0].message.content)
                
                # 結果の数がチャンクサイズと一致しない場合の処理
                if len(scores_and_reasons) != len(chunk):
                    print(f"警告: 結果数({len(scores_and_reasons)})がチャンクサイズ({len(chunk)})と一致しません")
                    # 必要に応じて空のスコアで埋める
                    while len(scores_and_reasons) < len(chunk):
                        scores_and_reasons.append(({}, ""))
                    # 余分な結果を切り詰める
                    scores_and_reasons = scores_and_reasons[:len(chunk)]
                
                return scores_and_reasons
            except Exception as e:
                print(f"スコアリングエラー: {e}")
                traceback.print_exc()
                return [(None, None)] * len(chunk)

        # テキストをチャンクに分割
        chunks = [texts[i:i + self.batch_size] for i in range(0, len(texts), self.batch_size)]
        tasks = [process_chunk(chunk) for chunk in chunks]
        results = await asyncio.gather(*tasks)
        
        # 結果を平坦化
        flattened_results = [item for sublist in results for item in sublist]
        
        # テキスト数と結果数が一致するかチェック
        if len(flattened_results) != len(texts):
            print(f"警告: 結果の総数({len(flattened_results)})がテキスト数({len(texts)})と一致しません")
            # 必要に応じて空のスコアで埋める
            while len(flattened_results) < len(texts):
                flattened_results.append(({}, ""))
            # 余分な結果を切り詰める
            flattened_results = flattened_results[:len(texts)]
        
        return flattened_results

    def _create_scoring_prompt(self, texts):
        base_prompt = """
あなたは感情分析の専門家です、文脈に注目して一次感情を抽出し、0から5の範囲で評価してください。


【評価基準】
0：感情が全く感じられい
1：ごくわずかに感情が感じられる
2：感情が弱めだが感じられる
3：感情が明確に感じられる
4：はっきりと強い感情が表出
5：圧倒的で非常に強烈な感情

【評価の重要原則】
1. 純粋性：各感情は他の感情との混合ではなく、純粋な形で評価する。
2. 文脈性：表現の背景にある状況や文脈を十分に考慮する。
3. 総合性：言語表現と非言語的要素を総合的に判断する。
4. 直接性：直接的な表現と間接的な表現の強度を適切に比較評価する。
5. 文化考慮：日本語特有の遠回しな表現や皮肉、婉曲表現の文化的背景を考慮する。

分析対象の文章:
"""
        for i, text in enumerate(texts, 1):
            base_prompt += f"\n投稿ID: {i}\n{str(text)}\n"
        
        base_prompt += """
以下の形式で各投稿について出力してください。必ず正確に以下のフォーマットに従ってください：

投稿ID: {ID}
感情スコア:
- 喜び: {スコア}
- 悲しみ: {スコア}
- 恐れ: {スコア}
- 驚き: {スコア}
- 怒り: {スコア}
- 嫌悪: {スコア}
感情全体の理由: {理由}

注意事項：
- 各感情のスコアは0～5の数値で表してください（小数点不可）
- 感情名は「喜び」「悲しみ」「恐れ」「驚き」「怒り」「嫌悪」の表記を厳守してください
- 各投稿に対して必ず全ての感情スコアを記入してください
- フォーマットを厳密に守ってください、追加の説明や考察は不要です"""
        return base_prompt

    def _parse_batch_response(self, response_text):
        print("OpenAIレスポンスのパース開始...")
        print(f"レスポンス: {response_text[:200]}...") # 最初の200文字だけ表示
        
        results = []
        current_id = None
        current_scores = {}
        current_reason = None
        processing_scores = False
        
        lines = response_text.split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
                
            print(f"処理中の行: {line}")
            
            # 投稿IDを検出
            if line.startswith('投稿ID:'):
                # 前の結果があれば保存
                if current_id is not None:
                    print(f"投稿 {current_id} のスコア: {current_scores}")
                    print(f"投稿 {current_id} の理由: {current_reason}")
                    results.append((current_scores, current_reason or ""))
                
                current_id = line.replace('投稿ID:', '').strip()
                current_scores = {
                    '喜び': 0.0,
                    '悲しみ': 0.0,
                    '恐れ': 0.0,
                    '驚き': 0.0,
                    '怒り': 0.0,
                    '嫌悪': 0.0
                }
                current_reason = None
                processing_scores = False
            
            # 感情スコアセクションの開始を検出
            elif line.startswith('感情スコア:'):
                processing_scores = True
            
            # 各感情スコアを処理
            elif processing_scores and line.startswith('- '):
                try:
                    line = line[2:].strip()  # "- " を削除して先頭と末尾の空白を取り除く
                    if ':' in line:
                        emotion, score_str = line.split(':', 1)
                        emotion = emotion.strip()
                        score_str = score_str.strip()
                        
                        # 数値に変換可能か確認
                        try:
                            score = float(score_str)
                            print(f"感情: {emotion}, スコア: {score}")
                            
                            # 正規化された感情名に変換
                            normalized_emotion = emotion
                            # 大文字小文字やスペースの違いを考慮
                            if '喜' in emotion or 'joy' in emotion.lower():
                                normalized_emotion = '喜び'
                            elif '悲' in emotion or 'sad' in emotion.lower():
                                normalized_emotion = '悲しみ'
                            elif '恐' in emotion or 'fear' in emotion.lower():
                                normalized_emotion = '恐れ'
                            elif '驚' in emotion or 'surprise' in emotion.lower():
                                normalized_emotion = '驚き'
                            elif '怒' in emotion or 'anger' in emotion.lower():
                                normalized_emotion = '怒り'
                            elif '嫌' in emotion or 'disgust' in emotion.lower():
                                normalized_emotion = '嫌悪'
                            
                            current_scores[normalized_emotion] = score
                        except ValueError:
                            print(f"スコア変換エラー: '{score_str}'は数値ではありません")
                except Exception as e:
                    print(f"スコアの解析エラー: {line} - {e}")
            
            # 感情の理由を検出
            elif line.startswith('感情全体の理由:'):
                current_reason = line.replace('感情全体の理由:', '').strip()
                processing_scores = False
        
        # 最後の結果を追加
        if current_id is not None:
            print(f"最後の投稿 {current_id} のスコア: {current_scores}")
            print(f"最後の投稿 {current_id} の理由: {current_reason}")
            results.append((current_scores, current_reason or ""))
        
        print(f"パース完了。結果数: {len(results)}")
        return results

    async def analyze_file(self, progress_callback=None):
        if self.df is None:
            print("エラー: DataFrameがありません")
            return False
            
        if '投稿内容' not in self.df.columns:
            print("エラー: '投稿内容'列がDataFrameにありません")
            column_info = f"利用可能な列: {', '.join(self.df.columns)}"
            print(column_info)
            return False

        # NaN値を空の文字列に置き換え
        self.df['投稿内容'] = self.df['投稿内容'].fillna('').astype(str)
        
        texts = self.df['投稿内容'].tolist()
        total_texts = len(texts)
        
        if total_texts == 0:
            print("警告: 分析するテキストがありません")
            return False
        
        print(f"合計 {total_texts} 件のテキストを分析します")
        
        # 空でないテキストのみをバッチに分割
        non_empty_indices = [i for i, text in enumerate(texts) if text.strip()]
        non_empty_texts = [texts[i] for i in non_empty_indices]
        
        if not non_empty_texts:
            print("警告: 空でないテキストがありません")
            return False
        
        # バッチに分割
        batches = [non_empty_texts[i:i + self.batch_size] for i in range(0, len(non_empty_texts), self.batch_size)]
        
        all_results = {}
        for i in range(total_texts):
            all_results[i] = {
                'moderation': None,
                'emotions': (None, None)
            }
        
        for i, batch in enumerate(batches):
            batch_indices = non_empty_indices[i * self.batch_size:(i + 1) * self.batch_size]
            
            try:
                print(f"バッチ {i+1}/{len(batches)} の処理を開始 ({len(batch)}件)")
                moderation_task = self.moderate_text_batch(batch)
                scoring_task = self.get_aggressiveness_scores_batch(batch)
                
                batch_categories, batch_emotions = await asyncio.gather(moderation_task, scoring_task)
                
                print(f"バッチ {i+1} 処理完了: モデレーション結果 {len(batch_categories)}件, 感情分析結果 {len(batch_emotions)}件")
                
                # バッチサイズと結果数を確認
                if len(batch_categories) != len(batch):
                    print(f"警告: モデレーション結果数({len(batch_categories)})がバッチサイズ({len(batch)})と一致しません")
                    # 必要に応じて調整
                    batch_categories = batch_categories[:len(batch)]
                    while len(batch_categories) < len(batch):
                        batch_categories.append(None)
                
                if len(batch_emotions) != len(batch):
                    print(f"警告: 感情分析結果数({len(batch_emotions)})がバッチサイズ({len(batch)})と一致しません")
                    # 必要に応じて調整
                    batch_emotions = batch_emotions[:len(batch)]
                    while len(batch_emotions) < len(batch):
                        batch_emotions.append((None, None))
                
                for j, (idx, category, emotion) in enumerate(zip(batch_indices, batch_categories, batch_emotions)):
                    all_results[idx] = {
                        'moderation': category,
                        'emotions': emotion
                    }
                    print(f"インデックス {idx} の結果: 感情={emotion[0] if emotion and emotion[0] else 'なし'}")
            except Exception as e:
                print(f"バッチ {i+1}/{len(batches)} の処理中にエラーが発生しました: {e}")
                traceback.print_exc()
            
            if progress_callback:
                progress = min(100, int((i + 1) * len(batch) * 100 / len(non_empty_texts)))
                progress_callback(progress)
        
        # 結果をDataFrameに追加
        print("DataFrameに結果を追加中...")
        category_names = ["hate", "hate/threatening", "self-harm", "sexual", "sexual/minors", "violence", "violence/graphic"]
        
        for name in category_names:
            self.df[f'{name}_flag'] = False
            self.df[f'{name}_score'] = 0.0
        
        emotion_columns = {
            '喜び': 'joy_score',
            '悲しみ': 'sadness_score',
            '恐れ': 'fear_score',
            '驚き': 'surprise_score',
            '怒り': 'anger_score',
            '嫌悪': 'disgust_score'
        }
        
        # まず感情スコア列を初期化
        for col in emotion_columns.values():
            self.df[col] = 0.0
        self.df['emotion_reason'] = ''
        
        # 結果をDataFrameに追加
        print("感情分析結果をDataFrameに追加中...")
        for i, result in all_results.items():
            if i < len(self.df):
                # モデレーション結果の追加
                if result['moderation']:
                    for name in category_names:
                        field_name = name.replace("/", "_")
                        try:
                            self.df.at[i, f'{name}_flag'] = getattr(result['moderation'].categories, field_name, False)
                            self.df.at[i, f'{name}_score'] = getattr(result['moderation'].category_scores, field_name, 0.0)
                        except Exception as e:
                            print(f"モデレーション結果の追加エラー: {e}")
                
                # 感情分析結果の追加
                scores, reason = result['emotions']
                try:
                    if scores:
                        print(f"インデックス {i} の感情スコア: {scores}")
                        for emotion_ja, col_name in emotion_columns.items():
                            score_value = scores.get(emotion_ja, 0.0)
                            print(f"  - {emotion_ja} -> {col_name}: {score_value}")
                            self.df.at[i, col_name] = score_value
                        
                        if reason:
                            self.df.at[i, 'emotion_reason'] = reason
                            print(f"  - 理由: {reason[:50]}...")
                except Exception as e:
                    print(f"感情分析結果の追加エラー (インデックス {i}): {e}")
                    traceback.print_exc()
        
        # 最終的なデータを確認
        print("感情分析スコアの列統計:")
        for col_name in emotion_columns.values():
            if col_name in self.df.columns:
                non_zero_count = (self.df[col_name] > 0).sum()
                print(f"  - {col_name}: 非ゼロ値の数={non_zero_count}, 平均={self.df[col_name].mean():.2f}")
            else:
                print(f"  - {col_name}: 列が存在しません")
        
        return True


class ModernModerationGUI:
    def __init__(self):
        self.tool = ModerationTool()
        
        # ウィンドウの設定
        self.root = ctk.CTk()
        self.root.title("感情分析ツール")
        self.root.geometry("600x400")
        
        # テーマの設定
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        
        # asyncioイベントループの作成
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        self.setup_gui()

    def setup_gui(self):
        # メインフレーム
        main_frame = ctk.CTkFrame(self.root)
        main_frame.pack(padx=20, pady=20, fill="both", expand=True)
        
        # タイトル
        title_label = ctk.CTkLabel(
            main_frame, 
            text="感情分析ツール", 
            font=("Helvetica", 24, "bold")
        )
        title_label.pack(pady=20)
        
        # ファイル情報ラベル
        self.file_info_label = ctk.CTkLabel(
            main_frame,
            text="ファイル: 未選択",
            font=("Helvetica", 12)
        )
        self.file_info_label.pack(pady=5)
        
        # ステータスラベル
        self.status_label = ctk.CTkLabel(
            main_frame,
            text="ステータス: 準備完了",
            font=("Helvetica", 12)
        )
        self.status_label.pack(pady=5)
        
        # APIキー検証ステータス
        self.api_status_label = ctk.CTkLabel(
            main_frame,
            text="APIキー: 未検証",
            font=("Helvetica", 12)
        )
        self.api_status_label.pack(pady=5)
        
        # プログレスバー
        self.progress = ctk.CTkProgressBar(main_frame)
        self.progress.pack(pady=20, padx=20, fill="x")
        self.progress.set(0)
        
        # ボタンフレーム
        button_frame = ctk.CTkFrame(main_frame)
        button_frame.pack(pady=20, fill="x")
        
        # ファイル選択ボタン
        load_button = ctk.CTkButton(
            button_frame,
            text="ファイルを選択",
            command=self.load_excel_file,
            width=150
        )
        load_button.pack(pady=10)
        
        # 分析開始ボタン
        self.analyze_button = ctk.CTkButton(
            button_frame,
            text="分析開始",
            command=self.start_analysis,
            width=150,
            state="disabled"
        )
        self.analyze_button.pack(pady=10)
        
        # 結果保存ボタン
        self.save_button = ctk.CTkButton(
            button_frame,
            text="結果を保存",
            command=self.save_results,
            width=150,
            state="disabled"
        )
        self.save_button.pack(pady=10)
        
        # APIキーの検証
        self.check_api_key()

    def check_api_key(self):
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            self.api_status_label.configure(text="APIキー: 設定されていません")
            messagebox.showwarning("警告", "OPENAI_API_KEYが環境変数に設定されていません。")
        else:
            self.api_status_label.configure(text=f"APIキー: {'*' * 5}{api_key[-4:]}")

    def update_progress(self, value):
        self.progress.set(value / 100)
        self.status_label.configure(text=f"ステータス: 分析中... {value}%")
        self.root.update_idletasks()

    def load_excel_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        if file_path:
            try:
                self.tool.df = pd.read_excel(file_path, sheet_name=0)
                
                # ファイル情報を表示
                num_rows = len(self.tool.df)
                file_name = os.path.basename(file_path)
                self.file_info_label.configure(text=f"ファイル: {file_name} ({num_rows}行)")
                
                # 投稿内容列のチェック
                if '投稿内容' in self.tool.df.columns:
                    self.status_label.configure(text="ステータス: ファイルを読み込みました")
                    self.analyze_button.configure(state="normal")
                    messagebox.showinfo("成功", f"ファイルを読み込みました。{num_rows}行のデータがあります。")
                else:
                    columns = ", ".join(self.tool.df.columns)
                    self.status_label.configure(text="ステータス: '投稿内容'列がありません")
                    messagebox.showwarning("警告", f"'投稿内容'列が見つかりません。\n\n利用可能な列: {columns}")
            except Exception as e:
                self.status_label.configure(text="ステータス: ファイル読み込みに失敗しました")
                messagebox.showerror("エラー", f"読み込みエラー: {e}")

    async def analyze_wrapper(self):
        try:
            success = await self.tool.analyze_file(self.update_progress)
            # GUIの更新はメインスレッドで行う
            self.root.after(0, self.update_after_analysis, success)
        except Exception as e:
            error_msg = f"分析中にエラーが発生しました: {e}"
            traceback.print_exc()
            # GUIの更新はメインスレッドで行う
            self.root.after(0, self.show_error, error_msg)

    def update_after_analysis(self, success):
        if success:
            self.status_label.configure(text="ステータス: 分析が完了しました")
            self.save_button.configure(state="normal")
            messagebox.showinfo("完了", "分析が完了しました")
        else:
            self.status_label.configure(text="ステータス: 分析に失敗しました")
            messagebox.showerror("エラー", "分析に失敗しました")

    def show_error(self, message):
        self.status_label.configure(text="ステータス: エラーが発生しました")
        messagebox.showerror("エラー", message)

    def start_analysis(self):
        if self.tool.df is None:
            messagebox.showerror("エラー", "ファイルを先にアップロードしてください")
            return

        # APIキーの確認
        if not os.getenv('OPENAI_API_KEY'):
            messagebox.showerror("エラー", "OPENAI_API_KEYが設定されていません")
            return

        self.status_label.configure(text="ステータス: 分析を実行中...")
        self.analyze_button.configure(state="disabled")
        self.save_button.configure(state="disabled")
        self.progress.set(0)
        
        # 非同期処理を別スレッドで実行
        threading.Thread(target=self.run_async_task, args=(self.analyze_wrapper(),), daemon=True).start()

    def run_async_task(self, coroutine):
        asyncio.run_coroutine_threadsafe(coroutine, self.loop)

    def save_results(self):
        if self.tool.df is None:
            messagebox.showerror("エラー", "保存する結果がありません")
            return

        # 感情分析の結果列が存在するか確認
        required_columns = ['joy_score', 'anger_score', 'emotion_reason']
        missing_columns = [col for col in required_columns if col not in self.tool.df.columns]
        
        if missing_columns:
            missing_cols_str = ", ".join(missing_columns)
            messagebox.showerror("エラー", f"分析結果が不完全です。以下の列がありません: {missing_cols_str}")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")]
        )
        if file_path:
            try:
                self.tool.df.to_excel(file_path, index=False)
                self.status_label.configure(text="ステータス: 結果を保存しました")
                messagebox.showinfo("成功", f"結果を{file_path}に保存しました")
            except Exception as e:
                self.status_label.configure(text="ステータス: 保存に失敗しました")
                messagebox.showerror("エラー", f"保存エラー: {e}")

    def run(self):
        # バックグラウンドでasyncioイベントループを実行
        def run_event_loop():
            asyncio.set_event_loop(self.loop)
            self.loop.run_forever()
        
        # イベントループを別スレッドで実行
        threading.Thread(target=run_event_loop, daemon=True).start()
        
        try:
            self.root.mainloop()
        finally:
            # ウィンドウが閉じられたらイベントループを停止
            self.loop.call_soon_threadsafe(self.loop.stop)

if __name__ == "__main__":
    gui = ModernModerationGUI()
    gui.run()
