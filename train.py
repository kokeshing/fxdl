import os
from keras.preprocessing.image import ImageDataGenerator
from keras.models import Sequential, Model
from keras.layers import Input, Activation, Dropout, Flatten, Dense
from keras import callbacks
from keras import optimizers
import numpy as np
from resnet import ResnetBuilder

batch_size = 300
num_classes = 3

img_rows, img_cols = 30, 30
channels = 3

train_data_dir = './train_data/'
validation_data_dir = './val_data/'
log_filepath = './log/'

num_train_samples = 100000
num_val_samples = 20000
num_epoch = 100

result_dir = './result/'
if not os.path.exists(result_dir):
    os.mkdir(result_dir)


if __name__ == '__main__':
    # Resnet50モデルを構築
    input_shape = [3, 30, 30]
    resnet50 = ResnetBuilder.build_resnet_50(input_shape, num_classes)
    model = Model(input=resnet50.input, output=resnet50.output)

    # SGD+momentumがいいらしい
    model.compile(loss='categorical_crossentropy',
                  optimizer=optimizers.SGD(lr=1e-2, decay=1e-6, momentum=0.9, nesterov=True),
                  metrics=['accuracy'])

    datagen = ImageDataGenerator(data_format="channels_first")

    train_generator = datagen.flow_from_directory(
        train_data_dir,
        target_size=(img_rows, img_cols),
        color_mode='rgb',
        class_mode='categorical',
        batch_size=batch_size,
        shuffle=True)

    validation_generator = datagen.flow_from_directory(
        validation_data_dir,
        target_size=(img_rows, img_cols),
        color_mode='rgb',
        class_mode='categorical',
        batch_size=batch_size,
        shuffle=True)

    cp_cb = callbacks.ModelCheckpoint(
        filepath = './result/model{epoch:02d}-loss{loss:.2f}-acc{acc:.2f}-vloss{val_loss:.2f}-vacc{val_acc:.2f}.h5',
        monitor='val_loss',
        verbose=1,
        save_best_only=False,
        mode='auto')

    tb_cb = callbacks.TensorBoard(
        log_dir=log_filepath,
        histogram_freq=0,
        write_graph=True,
        write_images=True)

    history = model.fit_generator(
        train_generator,
        samples_per_epoch=num_train_samples,
        nb_epoch=num_epoch,
        callbacks=[cp_cb, tb_cb],
        validation_data=validation_generator,
        nb_val_samples=num_val_samples)

    model.save_weights(os.path.join(result_dir, 'trained_model.h5'))
    save_history(history, os.path.join(result_dir, 'history.txt'))